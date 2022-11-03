//! 需要先指定分片，多条命令配合的测试

use crate::ci::env::*;
use crate::redis::RESTYPE;
use crate::redis_helper::*;
use function_name::named;
use redis::{Commands, RedisError};
use std::collections::HashSet;
use std::vec;

const SERVERS: [[&str; 2]; 4] = [
    ["127.0.0.1:56378", "127.0.0.1:56381"],
    ["127.0.0.1:56378", "127.0.0.1:56380"],
    ["127.0.0.1:56380", "127.0.0.1:56379"],
    ["127.0.0.1:56381", "127.0.0.1:56378"],
];
