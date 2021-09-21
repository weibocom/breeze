use std::sync::Arc;

pub struct Cid {
    faked: bool,
    id: usize,
    ids: Option<Arc<Ids>>,
}

impl Cid {
    pub fn fake() -> Self {
        Self {
            faked: true,
            id: std::usize::MAX,
            ids: None,
        }
    }
    pub fn new(id: usize, ids: Arc<Ids>) -> Self {
        Cid {
            faked: false,
            id: id,
            ids: Some(ids),
        }
    }
    #[inline(always)]
    pub fn id(&self) -> usize {
        self.id
    }
    #[inline(always)]
    pub fn faked(&self) -> bool {
        self.faked
    }
}
impl Drop for Cid {
    #[inline(always)]
    fn drop(&mut self) {
        if let Some(ids) = &self.ids {
            ids.release(self.id);
        }
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
