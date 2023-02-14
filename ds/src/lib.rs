pub mod chan;
mod cow;
pub mod lock;
mod mem;
pub mod queue;
pub mod rand;
pub mod utf8;
pub mod vec;
mod waker;

pub use cow::*;
pub use mem::*;
pub use vec::Buffer;
mod switcher;
pub use queue::PinnedQueue;
pub use switcher::Switcher;
pub use utf8::*;
pub use waker::AtomicWaker;

pub mod time;

mod asserts;
pub use asserts::*;

pub const NUM_STR_TBL: [&'static str; 32] = [
    "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16",
    "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31",
];

pub trait BufWriter {
    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()>;
    #[inline]
    fn write_seg_all(&mut self, buf0: &[u8], buf1: &[u8]) -> std::io::Result<()> {
        self.write_all(buf0)?;
        self.write_all(buf1)
    }
}
