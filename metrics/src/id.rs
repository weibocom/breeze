use std::collections::HashMap;
use std::sync::RwLock;

#[derive(Default)]
struct IdSequence {
    seq: usize,
    names: Vec<String>,
    indice: HashMap<String, usize>,
}

impl IdSequence {
    fn register_name(&mut self, name: String) -> usize {
        match self.indice.get(&name) {
            Some(seq) => *seq,
            None => {
                self.seq += 1;
                let seq = self.seq;
                if self.names.len() <= seq {
                    for _ in self.names.len()..=seq {
                        self.names.push("".to_string());
                    }
                }
                self.names[seq] = name;
                seq
            }
        }
    }
    fn name(&self, id: usize) -> &str {
        debug_assert!(id < self.names.len());
        &self.names[id]
    }
}

lazy_static! {
    static ref ID_SEQ: RwLock<IdSequence> = RwLock::new(IdSequence::default());
}

pub fn register_name(name: String) -> usize {
    ID_SEQ.write().unwrap().register_name(name)
}

pub fn get_name<'a>(id: usize) -> String {
    ID_SEQ.read().unwrap().name(id).to_string()
}
