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
    // let mut db = KvStore::new();

    match &cli.command {
        Commands::Get { key } => {
            eprintln!("unimplemented");
            std::process::exit(1);
        }
        Commands::Set { key, value} => {
            eprintln!("unimplemented");
            std::process::exit(1);
        }        
        Commands::Rm { key } => {
            eprintln!("unimplemented");
            std::process::exit(1);
        }                
    }
}
