#[derive(Clone, Default)]
pub struct Bkdr;

impl super::Hash for Bkdr {
    fn hash(&mut self, b: &[u8]) -> u64 {
        let mut h = 0i32;
        let seed = 31i32;
        for c in b.iter() {
            h = h.wrapping_mul(seed).wrapping_add(*c as i32);
        }
        if h < 0 {
            h = h.wrapping_mul(-1);
        }

        h as u64
    }
}
