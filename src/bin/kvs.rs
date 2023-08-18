use std::env::current_dir;
use assert_cmd::prelude::OutputOkExt;
use clap::App;
use kvs::KvStore;
use kvs::Result;

fn main() -> Result<()> {
    let matches = App::new("kv store")
        .subcommand(
            Subcommand::with_name("set")
                .about("Set the key and value")
                .arg(Arg::with_name("KEY").help("string key").required(true))
                .arg(Arg::with_name("VALUE")).help("string value").required(true),
        )
        .get_maches();
    
    ok(())
}
