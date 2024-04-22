mod ephemera;
pub use ephemera::*;

pub trait Add<T> {
    fn add(&mut self, t: T);
}

impl<T> Add<T> for Vec<T>
where
    T: std::cmp::PartialEq,
{
    #[inline]
    fn add(&mut self, e: T) {
        if !self.contains(&e) {
            self.push(e);
        }
    }
}
