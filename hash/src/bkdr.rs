#[derive(Clone, Default)]
pub struct Bkdr;

impl super::Hash for Bkdr {
    fn hash(&mut self, b: &[u8]) -> u64 {
        let mut h = 0usize;
        let seed = 31usize;
        for c in b.iter() {
            h = h.wrapping_mul(seed).wrapping_add(*c as usize);
        }
        h as u64
    }
}
