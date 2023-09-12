pub mod chan;
mod cow;
pub mod decrypt;
pub mod lock;
mod mem;
//pub mod queue;
pub mod rand;
pub mod utf8;
pub mod vec;
mod waker;

pub use cow::*;
pub use mem::*;
pub use vec::Buffer;
mod switcher;
//pub use queue::PinnedQueue;
pub use switcher::Switcher;
pub use utf8::*;
pub use waker::AtomicWaker;

pub mod time;

mod asserts;
pub use asserts::*;

pub trait BufWriter {
    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()>;
    #[inline]
    fn write_seg_all(&mut self, buf0: &[u8], buf1: &[u8]) -> std::io::Result<()> {
        self.write_all(buf0)?;
        self.write_all(buf1)
    }
}

pub trait NumStr {
    fn with_str<O>(&self, f: impl FnMut(&[u8]) -> O) -> O;
}

impl NumStr for usize {
    #[inline]
    fn with_str<O>(&self, mut f: impl FnMut(&[u8]) -> O) -> O {
        match *self {
            0..=9 => f(&[b'0' + *self as u8]),
            10..=99 => {
                let mut buf = [0u8; 2];
                buf[0] = b'0' + (*self / 10) as u8;
                buf[1] = b'0' + (*self % 10) as u8;
                f(&buf)
            }
            100..=999 => {
                let mut buf = [0u8; 3];
                buf[0] = b'0' + (*self / 100) as u8;
                buf[1] = b'0' + (*self / 10 % 10) as u8;
                buf[2] = b'0' + (*self % 10) as u8;
                f(&buf)
            }
            _ => {
                let mut buf = [0u8; 32];
                let mut left = *self;
                let mut idx = buf.len() - 1;
                while left > 0 {
                    buf[idx] = b'0' + (left % 10) as u8;
                    left /= 10;
                    idx -= 1;
                }
                // 因为进入到这个分支，self一定是大于等于10000的
                debug_assert!(idx <= buf.len() - 1);
                f(&buf[idx + 1..])
            }
        }
    }
}
