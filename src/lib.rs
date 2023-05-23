
pub struct KvStore;

impl KvStore {
    pub fn new() -> KvStore{
        KvStore
    }

    pub fn set(&mut self, key: String, value: String){
        eprintln!("unimplemented");
        std::process::exit(1);
    }

    pub fn get(&self, key: &String) -> Option<&String>{
        eprintln!("unimplemented");
        std::process::exit(1);
    }

    pub fn remove(&mut self, key: &String){
        panic!()
    }
}
