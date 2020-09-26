use anyhow::Result;
use clap::{App, Arg, SubCommand};
use kvs::{Error, KvStore};
use std::process::exit;

#[allow(unreachable_code)]
#[allow(unused_variables)]
#[allow(unused_mut)]
fn main() -> Result<()> {
    let opt = App::new("kvs")
        .author(env!("CARGO_PKG_AUTHORS"))
        .version(env!("CARGO_PKG_VERSION"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .arg(
            Arg::with_name("version")
                .short("V")
                .help("show version info"),
        )
        .subcommand(
            SubCommand::with_name("get")
                .about("get value with key")
                .arg(Arg::with_name("key")),
        )
        .subcommand(
            SubCommand::with_name("set")
                .about("set value with key")
                .args(&[Arg::with_name("key"), Arg::with_name("value")]),
        )
        .subcommand(
            SubCommand::with_name("rm")
                .about("remove key")
                .arg(Arg::with_name("key")),
        )
        .get_matches();

    let mut kv_store = KvStore::open(".")?;

    match opt.subcommand() {
        ("get", Some(get_sub)) => {
            let key = get_sub.value_of("key").unwrap();
            if let Some(value) = kv_store.get(key.to_owned())? {
                println!("{}", value);
            } else {
                println!("Key not found");
            }
        }
        ("set", Some(set_sub)) => {
            let key = set_sub.value_of("key").unwrap();
            let value = set_sub.value_of("value").unwrap();
            kv_store.set(key.to_owned(), value.to_owned())?;
        }
        ("rm", Some(rm_sub)) => {
            let key = rm_sub.value_of("key").unwrap();
            kv_store.remove(key.to_owned()).or_else(|err| {
                if let Error::KeyNotFound(_) = err {
                    println!("Key not found");
                    exit(-1);
                }
                Err(err)
            });
        }
        _ => {
            if opt.is_present("version") {
                println!(env!("CARGO_PKG_VERSION"))
            } else {
                panic!()
            }
        }
    };

    Ok(())
}
