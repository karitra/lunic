extern crate cocaine;
extern crate futures;
extern crate time;
extern crate tokio_core;
extern crate yaml_rust;

#[macro_use] extern crate serde_derive;
extern crate serde;
extern crate serde_json;
#[macro_use] extern crate clap;

mod config;
mod dummy;
mod engine;
mod secure;

use clap::{App, Arg};


const DEFAULT_NODES_COUNT : u32 = 100;
const DEFAULT_SLEEP_DURATION : u64 = 10;
const DEFAULT_FILL_COUNT : u32 = 100;

const DEFAULT_PARENT_PATH : &str =  "/darkvoice/nodes";


const CONFIG_FILES: &[&'static str] = &[
    "/etc/cocaine/.cocaine/tools.yml",
];


fn main() {
    let options = App::new("Cocaine rust-fw api tester")
        .version(crate_version!())
        .arg(Arg::with_name("nodes")
            .short("n")
            .takes_value(true)
            .required(true)
            .help("nodes to create"))
        .arg(Arg::with_name("path")
            .short("p")
            .takes_value(true)
            .required(true)
            .help("parent path for nodes"))
        .arg(Arg::with_name("sleep")
            .short("s")
            .takes_value(true)
            .help("sleep for sepcified seconds after nodes creation"))
        .arg(Arg::with_name("fill")
            .short("f")
            .takes_value(true)
            .help("dummy records count within on node"))
        .get_matches();

    let config = config::Config::new_from_files(CONFIG_FILES);

    let nodes = value_t!(options, "nodes", u32).unwrap_or(DEFAULT_NODES_COUNT);
    let path  = value_t!(options, "path", String).unwrap_or(DEFAULT_PARENT_PATH.to_string());
    let sleep_for = value_t!(options, "sleep", u64).unwrap_or(DEFAULT_SLEEP_DURATION);
    let fill = value_t!(options, "fill", u32).unwrap_or(DEFAULT_FILL_COUNT);

    engine::create_sleep_remove(&config, &path, sleep_for, nodes, fill)
}
