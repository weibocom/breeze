//mod controller;
//pub use controller::{Controller, GroupStream};

mod parser;
pub use parser::*;
pub mod chan;
pub mod memcache;
mod slice;
pub use slice::RingSlice;

pub trait ResponseParser {
    fn parse_response(&mut self, response: &RingSlice) -> (bool, usize);
    fn probe_response_found(&mut self, response: &RingSlice) -> bool;
}
