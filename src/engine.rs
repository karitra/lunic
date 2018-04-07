use secure;
use config::Config;
use tokio_core::reactor::{
    Core,
    Handle,
    Timeout
};

use serde::{
    Serialize,
    Deserialize
};

use futures::{
    Future,
    Stream,
};
use futures::future::join_all;

use std::convert::AsRef;
use std::collections::HashMap;
use std::rc::Rc;
use std::time;
use std::io;

use cocaine::Service;
use cocaine::service::Unicorn;
use cocaine::hpack::RawHeader;
use cocaine::{
    Error,
};

use dummy;

const ZVM: &str = "tvm";

type AuthHeaders = Vec<RawHeader>;


#[derive(Debug)]
pub enum CombinedError {
    IOError(io::Error),
    CocaineError(Error),
}


fn make_dummy_table(size: u32) -> HashMap<String, dummy::DummyRecord> {
    let mut init_data = HashMap::new();

    for i in 0..size {
        let key = format!("some_{}", i);
        init_data.insert(key, dummy::DummyRecord::new(i as i32));
    }

    init_data
}


fn make_auth_headers(header: Option<String>) -> Option<AuthHeaders>  {
    header.and_then(|hdr|
        Some(vec![ RawHeader::new("authorization".as_bytes(), hdr.into_bytes()) ])
    )
}

// TODO: make use of AsRef<str>
pub fn create_nodes<'a, S, T>(unicorn: &'a Unicorn, config: &Config, prefixes: &'a [S], init_data: &'a T, handle: Handle)
    -> Box<Future<Item=bool, Error=CombinedError> + 'a>
where
    S: AsRef<str> + 'a,
    for<'de> T: Deserialize<'de> + Serialize + 'a
 {

    let mut completions = Vec::with_capacity(prefixes.len());
    let mut proxy = secure::make_ticket_service(Service::new(ZVM, &handle), &config);

    let completions = proxy.ticket_as_header()
        .and_then(move |header| {
            let headers = make_auth_headers(header);
            for prefix in prefixes.clone() {
                let created = unicorn.create(prefix.as_ref(), init_data, headers.clone());
                completions.push(created);
            }

            // Ok(completions)
            join_all(completions)
        });

    let result = completions
        .map_err(CombinedError::CocaineError)
        .and_then(|is_created| {
            let is_all_done = is_created.into_iter().all(|x| x.unwrap_or(false));
            Ok(is_all_done)
        });

    Box::new(result)
}

fn remove_with_subnodes<'a>(unicorn: &'a Unicorn, config: &Config, path: &'a str, handle: Handle)
    -> Box<Future<Item=bool, Error=CombinedError> + 'a>
{
    let mut proxy = secure::make_ticket_service(Service::new(ZVM, &handle), &config);

    let result = proxy.ticket_as_header()
        .and_then(move |header| {
            let headers = make_auth_headers(header);
            let hdr_to_move1 = headers.clone();
            let hdr_to_move2 = headers.clone();
            let hdr_to_move3 = headers.clone();
            let hdr_to_move4 = headers.clone();

            unicorn.children_subscribe(path, headers.clone())
                .and_then(move |(tx, stream)| {

                    println!("got stream");

                    let to_delete = Vec::new();
                    let to_delete_rc = Rc::new(to_delete);
                    let to_delete_rc_stream = Rc::clone(&to_delete_rc);

                    stream.for_each(move |(version, nodes)| {
                        println!("version {}", version);

                        let mut to_delete_rc = Rc::clone(&to_delete_rc_stream);

                        for node in &nodes {
                            println!("{:?}", node);
                            let path = format!("{}/{}", path, node);
                            Rc::get_mut(&mut to_delete_rc).unwrap().push(path);
                        }

                        Ok(())
                    })
                    .and_then(move |_| {
                        drop(tx);
                        Ok(to_delete_rc)
                    })
                })
                .and_then(move |to_delete_rc| {

                    let mut get = Vec::new();
                    for node in to_delete_rc.iter() {
                        let node_path_copy = node.clone();
                        let f = unicorn.get::<dummy::DummyRecord,_>(node, hdr_to_move1.clone())
                            .and_then(|(_, version)| {
                                Ok((node_path_copy, version))
                            });
                        get.push(f);
                    }
                    join_all(get)
                })
                .and_then(move |delete_task| {
                    let mut del = Vec::new();
                    for (node, version) in delete_task {
                        let f = unicorn.del(&node, &version, hdr_to_move2.clone());
                        del.push(f);
                    }

                    join_all(del)
                })
                .and_then(move |flags| {
                    let v = flags.iter().all(|x| x.unwrap_or(false));
                    Ok(v)
                })
                .and_then(move |is_subs_removed| {
                    unicorn.get::<Vec<String>,_>(path, hdr_to_move3)
                        .and_then(move |(_,version)| {
                            Ok((version, is_subs_removed))
                        })
                })
                .and_then(move |(version, is_subs_removed)| {
                    unicorn.del(path, &version, hdr_to_move4)
                        .and_then(move |is_removed| match is_removed {
                            Some(is_removed) => Ok(is_removed && is_subs_removed),
                            None => Ok(false)
                        })
                })
        })
        .map_err(CombinedError::CocaineError);

    Box::new(result)
}


pub fn create_sleep_remove(config: &Config, path: &str, to_sleep: u64, count: u32, size: u32) {
    let parent = &[path];
    let init_data = make_dummy_table(size);

    let subnodes = (0..count)
        .map(|i| {
            format!("{}/node_{}", path, i)
        })
        .collect::<Vec<String>>();

    let mut core = Core::new().unwrap();
    let handle = core.handle();

    let unicorn = Unicorn::new(Service::new("unicorn", &handle));

    let handle = core.handle();
    let create = create_nodes(&unicorn, &config, parent, &init_data, handle.clone())
        .and_then(|_| {
            println!("creating subnodes in the {}", path);
            create_nodes(&unicorn, &config, &subnodes, &init_data, handle.clone())
        });

    let sleep = create
        .and_then(|ok| {
            println!("sleeping for {} seconds, create result is {:?}", to_sleep, ok);
            Timeout::new(time::Duration::from_secs(to_sleep), &handle).unwrap()
                .map_err(CombinedError::IOError)
        });

    let complete = sleep
        .and_then(|_| {
            println!("time to remove dummy nodes from {}", path);
            remove_with_subnodes(&unicorn, config, path, handle.clone())
        });

    match core.run(complete) {
        Ok(result) => println!("result of complete operation: {:?}", result),
        Err(e) => println!("error while removing nodes {:?}", e)
    }
}
