use std::env::current_dir;

use clap::{Parser, Subcommand};
use kvs::KvStore;
use kvs::Result;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// get value from key
    Get { key: String },
    /// set key and value
    Set { key: String, value: String },
    /// remove key and value
    Rm { key: String },
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Get { key } => {
            let key = key.to_owned().as_str();
            let store = KvStore::open(current_dir()?)?;
            
        }
        Commands::Set { key: _, value: _ } => {
            eprintln!("unimplemented");
            std::process::exit(1);
        }
        Commands::Rm { key: _ } => {

        }
    }
}
