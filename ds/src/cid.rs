use std::sync::Arc;

pub struct Cid {
    id: usize,
    ids: Arc<Ids>,
}

impl Cid {
    pub fn new(id: usize, ids: Arc<Ids>) -> Self {
        Cid { id, ids }
    }
    #[inline(always)]
    pub fn id(&self) -> usize {
        self.id
    }
}
impl Drop for Cid {
    fn drop(&mut self) {
        self.ids.release(self.id);
    }
}

use std::sync::atomic::{AtomicBool, Ordering};
pub struct Ids {
    bits: Vec<AtomicBool>,
}

impl Ids {
    pub fn with_capacity(cap: usize) -> Self {
        log::debug!("ids builded, cap:{}", cap);
        Self {
            bits: (0..cap).map(|_| AtomicBool::new(false)).collect(),
        }
    }
    pub fn next(&self) -> Option<usize> {
        for (id, status) in self.bits.iter().enumerate() {
            match status.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => {
                    log::debug!("cid: next connection id success.  cap:{}", self.bits.len());
                    return Some(id);
                }
                Err(_) => {}
            }
        }
        log::debug!("cid: fetch next connection build failed. ");
        None
    }

    #[inline(always)]
    pub fn release(&self, id: usize) {
        unsafe {
            match self.bits.get_unchecked(id).compare_exchange(
                true,
                false,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {}
                Err(_) => panic!("not a valid status."),
            }
        }
    }
}
