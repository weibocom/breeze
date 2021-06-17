#[derive(Clone)]
pub struct Bkdr {
    hash: i32,
}

impl Bkdr {
    pub fn new() -> Self {
        Self {
            hash: 0,
        }
    }

    pub fn write(&mut self, bytes: &[u8]) {
        let seed = 32;
        for c in bytes {
            self.hash = self.hash * seed + *c as i32;
        }
    }

    pub fn finish(&self) -> u64 {
        if self.hash >= 0 {
            return self.hash as u64;
        }
        return (-1 * self.hash) as u64;
    }
}

