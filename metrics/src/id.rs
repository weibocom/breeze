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
                log::info!("metrics-register: name:{} id:{}", name, seq);
                self.names[seq] = name.to_owned();
                self.indice.insert(name, self.seq);
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
    ID_SEQ.write().unwrap().register_name(name.to_string())
}

pub fn register_names(names: Vec<&str>) -> usize {
    let mut s: Vec<u8> = Vec::with_capacity(32);
    for name in names.iter() {
        s.reserve(name.len());
        use std::ptr::copy_nonoverlapping as copy;
        unsafe {
            let offset = s.len() as isize;
            copy(name.as_ptr(), s.as_mut_ptr().offset(offset), name.len());
            s.set_len(s.len() + name.len());
            s.push(b'.');
        }
    }
    s.pop();

    unsafe { register_name(String::from_utf8_unchecked(s)) }
}

pub fn get_name(id: usize) -> String {
    ID_SEQ.read().unwrap().name(id).to_string()
}
pub fn with_name<F: Fn(&str)>(id: usize, f: F) {
    f(ID_SEQ.read().unwrap().name(id));
}

// 这个id是通过register_name生成
pub fn unregister_by_id(_id: usize) {}
