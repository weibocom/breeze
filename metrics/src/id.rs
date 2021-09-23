use std::collections::HashMap;
use std::sync::RwLock;

pub struct MetricId(usize);

impl MetricId {
    #[inline(always)]
    pub fn id(&self) -> usize {
        self.0
    }
    #[inline(always)]
    pub fn name(&self) -> String {
        name(self.0)
    }
    #[inline(always)]
    pub fn with<F: Fn(&str) -> O, O>(&self, f: F) -> O {
        f(ID_SEQ.read().unwrap().name(self.0))
    }
}

pub trait MetricName {
    fn name(&self) -> String;
    fn with<F: Fn(&str) -> O, O>(&self, f: F) -> O;
}

impl MetricName for usize {
    #[inline(always)]
    fn name(&self) -> String {
        name(*self)
    }
    #[inline(always)]
    fn with<F: Fn(&str) -> O, O>(&self, f: F) -> O {
        f(ID_SEQ.read().unwrap().name(*self))
    }
}

struct IdSequence {
    seq: usize,
    names: Vec<String>,
    indice: HashMap<String, usize>,
}

impl IdSequence {
    fn new() -> Self {
        // 初始化里面metric id是0表示，所有业务共享的元数据信息
        Self {
            seq: 1,
            names: vec!["mesh".to_string()],
            indice: Default::default(),
        }
    }
    fn register_name(&mut self, name: &str) -> usize {
        match self.indice.get(name) {
            Some(seq) => *seq,
            None => {
                let seq = self.seq;
                if self.names.len() <= seq {
                    for _ in self.names.len()..=seq {
                        self.names.push("".to_string());
                    }
                }
                log::debug!("metrics-register: name:{} id:{}", name, seq);
                self.names[seq] = name.to_owned();
                self.indice.insert(name.to_owned(), self.seq);
                self.seq += 1;
                seq
            }
        }
    }
    fn name(&self, id: usize) -> &str {
        debug_assert!(id < self.names.len());
        &self.names[id]
    }
}

#[macro_export]
macro_rules! register {
    ($name:expr) => {
        metrics::register_name($name)
    };
    ($first:expr, $($left:expr),+) => {
        metrics::register_names(vec![$first, $($left),+])
    };
}

lazy_static! {
    static ref ID_SEQ: RwLock<IdSequence> = RwLock::new(IdSequence::new());
}

pub fn register_name(name: &str) -> usize {
    ID_SEQ
        .write()
        .unwrap()
        .register_name(&crate::encode_addr(name))
}

pub fn register_names(names: Vec<&str>) -> usize {
    let mut s: String = String::with_capacity(32);
    for name in names.iter() {
        s += &crate::encode_addr(name);
        s.push('.');
    }
    s.pop();
    register_name(&s)
}

#[inline(always)]
pub fn name(id: usize) -> String {
    get_name(id)
}

#[inline(always)]
pub fn get_name(id: usize) -> String {
    ID_SEQ.read().unwrap().name(id).to_string()
}
pub fn with_name<F: Fn(&str)>(id: usize, f: F) {
    f(ID_SEQ.read().unwrap().name(id));
}

// 这个id是通过register_name生成
pub fn unregister_by_id(_id: usize) {}
