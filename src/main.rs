#[macro_use]
extern crate clap;
use clap::App;
use std::process::exit;
use kvs::{KvStore, Result};
use kvs::err::KvError;
use std::env;
use kvs::err::KvError::KeyNotFound;


fn main() -> Result<()> {
    let yaml = load_yaml!("cli.yml");
    let mut cfg = App::from_yaml(yaml).version(env!("CARGO_PKG_VERSION"));
    let matches = cfg.get_matches();
    let dir = env::current_dir()?;
    match matches.subcommand() {
        ("set", Some(set_matches)) => {
            let key = set_matches.value_of("KEY").expect("key is required");
            let value = set_matches.value_of("VALUE").expect("value is required");
            let mut store = KvStore::open(dir)?;
            if let Err(e) = store.set(key.to_owned(), value.to_owned()) {
                println!("{:?}", e);
            }
        },
        ("get", Some(get_matches)) => {
            let key = get_matches.value_of("KEY").expect("key is required");
            let mut store = KvStore::open(dir)?;
            let value = store.get(key.to_owned())?.unwrap_or("Key not found".to_owned());
            println!("{}", value);
        },
        ("rm", Some(rm_mathces)) => {
            let key = rm_mathces.value_of("KEY").expect("key is required");
            let mut store = KvStore::open(dir)?;
            if let Err(e) = store.remove(key.to_owned()) {
                if matches!(e, KvError::KeyNotFound) {
                    println!("Key not found");
                    exit(1);
                } else {
                    println!("{:?}", e);
                }
            }
        },
        ("", None) => {
            eprintln!("unimplemented");
            exit(1);
        },
        _ => {
            unreachable!()
        }
    }
    Ok(())
}
