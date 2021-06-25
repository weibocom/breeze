#[derive(Clone, Default)]
pub struct Bkdr;

impl super::Hash for Bkdr {
    fn hash(&mut self, b: &[u8]) -> u64 {
        let mut h: i32 = 0;
        let seed = 31;
        for c in b.iter() {
            h = h * seed + *c as i32;
        }
        if h < 0 {
            return (-1 * h) as u64;
        } else {
            return h as u64;
        }
    }
}
