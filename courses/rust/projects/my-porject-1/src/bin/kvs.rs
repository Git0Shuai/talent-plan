use clap::{App, Arg, SubCommand};
use kvs::KvStore;

fn main() {
    let opt = App::new("kvs")
        .arg(Arg::with_name("version").short("V"))
        .subcommand(SubCommand::with_name("get").arg(Arg::with_name("key")))
        .subcommand(
            SubCommand::with_name("set").args(&[Arg::with_name("key"), Arg::with_name("value")]),
        )
        .subcommand(SubCommand::with_name("rm").arg(Arg::with_name("key")))
        .get_matches();

    let mut kv_store = KvStore::new();

    match opt.subcommand() {
        ("get", Some(get_sub)) => {
            let key = get_sub.value_of("key").unwrap();
            kv_store.get(key.to_owned());
        }
        ("set", Some(set_sub)) => {
            let key = set_sub.value_of("key").unwrap();
            let value = set_sub.value_of("value").unwrap();
            kv_store.set(key.to_owned(), value.to_owned());
        }
        ("rm", Some(rm_sub)) => {
            let key = rm_sub.value_of("key").unwrap();
            kv_store.remove(key.to_owned());
        }
        _ => {
            if opt.is_present("version") {
                println!(env!("CARGO_PKG_VERSION"))
            } else {
                panic!()
            }
        }
    };

    println!("Hello, world!");
}
