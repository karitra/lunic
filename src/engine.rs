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
use std::cell::RefCell;
use std::rc::Rc;
use std::io;
use std::fmt;
use std::time::Duration;

use time;

use cocaine::Service;
use cocaine::service::Unicorn;
use cocaine::hpack::RawHeader;
use cocaine::Error;

use dummy;

const ZVM: &str = "tvm";

type AuthHeaders = Vec<RawHeader>;

#[derive(Debug)]
pub enum CombinedError {
    IOError(io::Error),
    CocaineError(Error),
}

fn make_auth_headers(header: Option<String>) -> AuthHeaders  {
    match header {
        Some(header) => vec![ RawHeader::new("authorization".as_bytes(), header.into_bytes()) ],
        None => vec![]
    }
}

// TODO: make use of AsRef<str>
pub fn create_nodes<'a, S, T>(unicorn: &'a Unicorn, config: &Config, prefixes: &'a [S], init_data: &'a T, handle: Handle)
    -> Box<Future<Item=bool, Error=CombinedError> + 'a>
where
    S: AsRef<str> + fmt::Display + 'a,
    for<'de> T: Deserialize<'de> + Serialize + 'a
 {
    let mut proxy = secure::make_ticket_service(Service::new(ZVM, &handle), &config);

    let result = proxy.ticket_as_header()
        .and_then(move |header| {
            let mut completions = Vec::with_capacity(prefixes.len());
            let headers = make_auth_headers(header);

            // println!("got headers {:?}", headers);

            for prefix in prefixes.clone() {
                // println!("\tcreating for prefix {}", prefix);
                let created = unicorn.create(prefix.as_ref(), init_data, headers.clone());
                completions.push(created);
            }

            // println!("join on completion complete");
            join_all(completions)
        })
        .and_then(|is_created| {
            println!("create applied {} times", is_created.len());
            let is_all_done = is_created.into_iter().all(|x| x);
            Ok(is_all_done)
        })
        .or_else(|e| {
            println!("error while creating nodes {:?}", e);
            println!("skipping to next operation");
            Ok(false)
        })
        .map_err(CombinedError::CocaineError);

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

                    // println!("got stream");

                    let to_delete = Vec::new();
                    let to_delete_rc = Rc::new(RefCell::new(to_delete));
                    let to_delete_rc_stream = Rc::clone(&to_delete_rc);

                    stream.take(1).for_each(move |(_version, nodes)| {
                        // println!("version {}", version);

                        let to_delete_rc = Rc::clone(&to_delete_rc_stream);

                        for node in &nodes {
                            let path = format!("{}/{}", path, node);
                            // println!("\tpath {:?}", node);
                            to_delete_rc.borrow_mut().push(path);
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
                    let now = time::now();
                    for node in to_delete_rc.borrow().iter() {
                        // println!("\tgetting data for node {}", node);
                        let node_path_copy = node.clone();
                        let f = unicorn.get::<dummy::DummyTable,_>(node, hdr_to_move1.clone())
                            .and_then(|(_, version)| {
                                Ok((node_path_copy, version))
                            });
                        get.push(f);
                    }
                    join_all(get).and_then(move |vec| Ok((now, vec)))
                })
                .and_then(move |(start, delete_task)| {

                    let diff = time::now() - start;
                    println!("get of {} items took {}ms", delete_task.len(), diff.num_milliseconds());

                    // println!("delete_task task size {}", delete_task.len());
                    let mut del = Vec::new();
                    for (node, version) in delete_task {
                        // println!("posting delete for subnode {1} {0}", node, version);
                        // let f = unicorn.del(&node, &version, hdr_to_move2.clone());
                        let f = unicorn.del(&node, &version, hdr_to_move2.clone());
                        del.push(f);
                    }

                    join_all(del)
                })
                .and_then(move |flags| {
                    println!("{} subnodes deleted", flags.len());
                    let v = flags.iter().all(|x| *x);
                    Ok(v)
                })
                .and_then(move |is_subs_removed| {
                    println!("deleting main node {}", path);
                    unicorn.get::<dummy::DummyTable,_>(path, hdr_to_move3)
                        .and_then(move |(_,version)| {
                            Ok((version, is_subs_removed))
                        })
                })
                .and_then(move |(version, is_subs_removed)| {
                    println!("ready to delete parent node {}", path);
                    unicorn.del(path, &version, hdr_to_move4.clone())
                        .and_then(move |is_removed| Ok(is_removed && is_subs_removed))
                })
        })
        .map_err(CombinedError::CocaineError);

    Box::new(result)
}


pub fn create_sleep_remove(config: &Config, path: &str, to_sleep: u64, count: u32, size: u32) {
    let parent = &[path];
    let init_data = dummy::make_table(size);

    let subnodes = (0..count)
        .map(|i| {
            format!("{}/node_{}", path, i)
        })
        .collect::<Vec<String>>();

    let mut core = Core::new().unwrap();
    let handle = core.handle();

    let unicorn = Unicorn::new(Service::new("unicorn", &handle));

    let handle = core.handle();

    println!("creating parent node: {}", path);
    let create = create_nodes(&unicorn, &config, parent, &init_data, handle.clone())
        .and_then(|_| {
            println!("creating subnodes in the {}", path);
            create_nodes(&unicorn, &config, &subnodes, &init_data, handle.clone())
        });

    let sleep = create
        .and_then(|ok| {
            println!("sleeping for {} seconds, create result is {:?}", to_sleep, ok);
            Timeout::new(Duration::from_secs(to_sleep), &handle).unwrap()
                .map_err(CombinedError::IOError)
        });

    let complete = sleep
        .and_then(|_| {
            println!("time to remove dummy nodes from {}", path);
            remove_with_subnodes(&unicorn, config, path, handle.clone())
        });

    match core.run(complete) {
        Ok(result) => println!("result of complete operation: {:?}", result),
        Err(e) => println!("error while complete operation {:?}", e)
    }
}
