use clap::{Parser, Subcommand};
use kvs::KvStore;

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


fn main() {
    let cli = Cli::parse();
    let mut db = KvStore::new();

    match &cli.command {
        Commands::Get { key } => {
            db.get(&key);
        }
        Commands::Set { key, value} => {
            db.set(key.to_owned(), value.to_owned());
        }        
        Commands::Rm { key } => {
            db.remove(&key);
        }                
    }
}
