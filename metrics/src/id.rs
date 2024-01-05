use crate::MetricType;
#[derive(Debug, Hash, PartialEq, Eq, Default)]
pub struct Id {
    pub(crate) path: String,
    pub(crate) key: &'static str,
    pub(crate) t: MetricType,
}
impl Id {
    #[inline]
    pub(crate) fn valid(&self) -> bool {
        self.path.len() > 0 && !self.t.is_empty()
    }
}
pub(crate) const BASE_PATH: &str = "base";

use std::collections::HashMap;
use std::sync::Arc;
#[derive(Default, Clone)]
pub(crate) struct IdSequence {
    seq: usize,
    indice: HashMap<Arc<Id>, usize>,
}

impl IdSequence {
    pub(crate) fn get_idx(&self, id: &Arc<Id>) -> Option<usize> {
        self.indice.get(id).map(|idx| *idx)
    }
    pub(crate) fn register_name(&mut self, name: &Arc<Id>) -> usize {
        match self.indice.get(name) {
            Some(seq) => *seq,
            None => {
                let seq = self.seq;
                log::debug!("metric name registered. index:{} name:{}", seq, name.path);
                self.indice.insert(name.clone(), seq);
                self.seq += 1;
                seq
            }
        }
    }
}
