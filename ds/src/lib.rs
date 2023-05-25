pub mod chan;
mod cow;
pub mod lock;
mod mem;
pub mod queue;
pub mod rand;
pub mod utf8;
pub mod vec;
pub mod decrypt;
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

pub trait NumStr {
    fn with_str(&self, f: impl FnMut(&[u8]));
}

impl NumStr for usize {
    #[inline]
    fn with_str(&self, mut f: impl FnMut(&[u8])) {
        match *self {
            0..=9 => f(&[b'0' + *self as u8]),
            10..=99 => {
                let mut buf = [0u8; 2];
                buf[0] = b'0' + (*self / 10) as u8;
                buf[1] = b'0' + (*self % 10) as u8;
                f(&buf);
            }
            100..=999 => {
                let mut buf = [0u8; 3];
                buf[0] = b'0' + (*self / 100) as u8;
                buf[1] = b'0' + (*self / 10 % 10) as u8;
                buf[2] = b'0' + (*self % 10) as u8;
                f(&buf);
            }
            _ => {
                let mut buf = [0u8; 32];
                let mut left = *self;
                let idx = buf.len() - 1;
                while left > 0 {
                    buf[idx] = b'0' + (left % 10) as u8;
                    left /= 10;
                }
                f(&buf[idx..]);
            }
        }
    }
}

#[inline]
pub fn with_str(n: usize, mut f: impl FnMut(&[u8])) {
    let mut buf = [0u8; 32];
    let mut left = n;
    let idx = buf.len() - 1;
    while left > 0 {
        buf[idx] = b'0' + (left % 10) as u8;
        left /= 10;
    }
    f(&buf[idx..]);
}
