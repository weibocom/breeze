use crate::proto_hook;
use ds::BufWriter;
use protocol::redis::transmute;
use protocol::redis::Redis;
use protocol::Error::ProtocolIncomplete;
use protocol::StreamContext;

use protocol::BufRead;
const CTX: StreamContext = [0; 16];

#[test]
fn test_line() {
    let proto = Redis;

    let rspstr = b"-Error message\r\n";
    let mut stream = proto_hook::TestStream {
        oft: 0,
        inner: Vec::new(),
        ctx: Default::default(),
    };
    for i in 0..rspstr.len() {
        stream.write_all(&rspstr[i..i + 1]).unwrap();
        let rst = proto.parse_response_inner(&mut stream);
        if i < rspstr.len() - 1 {
            match rst {
                Err(ProtocolIncomplete(0)) => {}
                e => panic!("Expected ProtocolIncomplete, got: {:?}", e),
            }
            assert_eq!(stream.ctx, CTX);
        } else {
            assert!(rst.unwrap().unwrap().equal(rspstr));
            assert_eq!(stream.len(), 0);
            assert_eq!(stream.ctx, CTX);
        }
    }

    let rspstr = b":1000\r\n";
    for i in 0..rspstr.len() {
        stream.write_all(&rspstr[i..i + 1]).unwrap();
        let rst = proto.parse_response_inner(&mut stream);
        if i < rspstr.len() - 1 {
            match rst {
                Err(ProtocolIncomplete(0)) => {}
                e => panic!("Expected ProtocolIncomplete, got: {:?}", e),
            }
            assert_eq!(stream.ctx, CTX);
        } else {
            assert!(rst.unwrap().unwrap().equal(rspstr));
            assert_eq!(stream.len(), 0);
            assert_eq!(stream.ctx, CTX);
        }
    }

    let rspstr = b"+OK\r\n";
    for i in 0..rspstr.len() {
        stream.write_all(&rspstr[i..i + 1]).unwrap();
        let rst = proto.parse_response_inner(&mut stream);
        if i < rspstr.len() - 1 {
            match rst {
                Err(ProtocolIncomplete(0)) => {}
                e => panic!("Expected ProtocolIncomplete, got: {:?}", e),
            }
            assert_eq!(stream.ctx, CTX);
        } else {
            assert!(rst.unwrap().unwrap().equal(rspstr));
            assert_eq!(stream.len(), 0);
            assert_eq!(stream.ctx, CTX);
        }
    }
}

#[test]
fn test_string() {
    let proto = Redis;

    let rspstr = b"$6\r\nfoobar\r\n";
    let mut stream = proto_hook::TestStream {
        oft: 0,
        inner: Vec::new(),
        ctx: Default::default(),
    };
    for i in 0..rspstr.len() {
        stream.write_all(&rspstr[i..i + 1]).unwrap();
        let rst = proto.parse_response_inner(&mut stream);
        if i < rspstr.len() - 1 {
            let _left = if i < 3 { 0 } else { rspstr.len() - i - 1 };
            match rst {
                Err(ProtocolIncomplete(_left)) => {}
                e => panic!("Expected ProtocolIncomplete, got: {:?}", e),
            }
            assert_eq!(stream.ctx, CTX);
        } else {
            assert!(rst.unwrap().unwrap().equal(rspstr));
            assert_eq!(stream.len(), 0);
            assert_eq!(stream.ctx, CTX);
        }
    }

    let rspstr = b"$-1\r\n";
    for i in 0..rspstr.len() {
        stream.write_all(&rspstr[i..i + 1]).unwrap();
        let rst = proto.parse_response_inner(&mut stream);
        if i < rspstr.len() - 1 {
            match rst {
                Err(ProtocolIncomplete(0)) => {}
                e => panic!("Expected ProtocolIncomplete, got: {:?}", e),
            }
            assert_eq!(stream.ctx, CTX);
        } else {
            assert!(rst.unwrap().unwrap().equal(rspstr));
            assert_eq!(stream.len(), 0);
            assert_eq!(stream.ctx, CTX);
        }
    }
}

#[test]
fn test_bulk() {
    let proto = Redis;
    let mut stream = proto_hook::TestStream {
        oft: 0,
        inner: Vec::new(),
        ctx: Default::default(),
    };

    let rspstr = b"*0\r\n";
    for i in 0..rspstr.len() {
        stream.write_all(&rspstr[i..i + 1]).unwrap();
        let rst = proto.parse_response_inner(&mut stream);
        if i < rspstr.len() - 1 {
            match rst {
                Err(ProtocolIncomplete(64)) => {}
                e => panic!("Expected ProtocolIncomplete, got: {:?}", e),
            }
            let ctx = transmute(&mut stream.ctx);
            assert_eq!(ctx.bulk, 1);
            assert_eq!(ctx.oft, 0);
        } else {
            assert!(rst.unwrap().unwrap().equal(rspstr));
            assert_eq!(stream.len(), 0);
            assert_eq!(stream.ctx, CTX);
        }
    }

    let rspstr = b"*7\r\n$3\r\nfoo\r\n$-1\r\n:1\r\n+Foo\r\n+Bar\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Foo\r\n+Bar\r\n";
    for i in 0..rspstr.len() {
        stream.write_all(&rspstr[i..i + 1]).unwrap();
        let rst = proto.parse_response_inner(&mut stream);
        let ctx = transmute(&mut stream.ctx);
        match i {
            0..=2 => match rst {
                Err(ProtocolIncomplete(64)) => {
                    assert_eq!(ctx.bulk, 1);
                    assert_eq!(ctx.oft, 0);
                }
                e => panic!("Expected ProtocolIncomplete, got: {:?}", e),
            },
            3..=11 => match rst {
                Err(ProtocolIncomplete(left)) => {
                    let bulk = 7;
                    assert_eq!(left, bulk * 64);
                    assert_eq!(ctx.bulk, bulk);
                    assert_eq!(ctx.oft, 4);
                }
                e => panic!("Expected ProtocolIncomplete, got: {:?}", e),
            },
            12..=16 => match rst {
                Err(ProtocolIncomplete(left)) => {
                    let bulk = 6;
                    assert_eq!(left, bulk * 64);
                    assert_eq!(ctx.bulk, bulk);
                    assert_eq!(ctx.oft, 13);
                }
                e => panic!("Expected ProtocolIncomplete, got: {:?}", e),
            },
            17..=20 => match rst {
                Err(ProtocolIncomplete(left)) => {
                    let bulk = 5;
                    assert_eq!(left, bulk * 64);
                    assert_eq!(ctx.bulk, bulk);
                    assert_eq!(ctx.oft, 18);
                }
                e => panic!("Expected ProtocolIncomplete, got: {:?}", e),
            },
            21..=26 => match rst {
                Err(ProtocolIncomplete(left)) => {
                    let bulk = 4;
                    assert_eq!(left, bulk * 64);
                    assert_eq!(ctx.bulk, bulk);
                    assert_eq!(ctx.oft, 22);
                }
                e => panic!("Expected ProtocolIncomplete, got: {:?}", e),
            },
            27..=32 => match rst {
                Err(ProtocolIncomplete(left)) => {
                    let bulk = 3;
                    assert_eq!(left, bulk * 64);
                    assert_eq!(ctx.bulk, bulk);
                    assert_eq!(ctx.oft, 28);
                }
                e => panic!("Expected ProtocolIncomplete, got: {:?}", e),
            },
            33..=36 => match rst {
                Err(ProtocolIncomplete(left)) => {
                    let bulk = 2;
                    assert_eq!(left, bulk * 64);
                    assert_eq!(ctx.bulk, bulk);
                    assert_eq!(ctx.oft, 34);
                }
                e => panic!("Expected ProtocolIncomplete, got: {:?}", e),
            },
            // *7rn$3rnfoorn$-1rn:1rn+Foorn-Barrn*3rn:1rn:2rn:3rn*2rn+Foorn-Barrn
            37..=40 => match rst {
                Err(ProtocolIncomplete(left)) => {
                    let bulk = 4;
                    assert_eq!(left, bulk * 64);
                    assert_eq!(ctx.bulk, bulk);
                    assert_eq!(ctx.oft, 38);
                }
                e => panic!("Expected ProtocolIncomplete, got: {:?}", e),
            },
            41..=44 => match rst {
                Err(ProtocolIncomplete(left)) => {
                    let bulk = 3;
                    assert_eq!(left, bulk * 64);
                    assert_eq!(ctx.bulk, bulk);
                    assert_eq!(ctx.oft, 42);
                }
                e => panic!("Expected ProtocolIncomplete, got: {:?}", e),
            },
            45..=48 => match rst {
                Err(ProtocolIncomplete(left)) => {
                    let bulk = 2;
                    assert_eq!(left, bulk * 64);
                    assert_eq!(ctx.bulk, bulk);
                    assert_eq!(ctx.oft, 46);
                }
                e => panic!("Expected ProtocolIncomplete, got: {:?}", e),
            },
            49..=52 => match rst {
                Err(ProtocolIncomplete(left)) => {
                    let bulk = 1;
                    assert_eq!(left, bulk * 64);
                    assert_eq!(ctx.bulk, bulk);
                    assert_eq!(ctx.oft, 50);
                }
                e => panic!("Expected ProtocolIncomplete, got: {:?}", e),
            },
            53..=58 => match rst {
                Err(ProtocolIncomplete(left)) => {
                    let bulk = 2;
                    assert_eq!(left, bulk * 64);
                    assert_eq!(ctx.bulk, bulk);
                    assert_eq!(ctx.oft, 54);
                }
                e => panic!("Expected ProtocolIncomplete, got: {:?}", e),
            },
            59..=64 => match rst {
                Err(ProtocolIncomplete(left)) => {
                    let bulk = 1;
                    assert_eq!(left, bulk * 64);
                    assert_eq!(ctx.bulk, bulk);
                    assert_eq!(ctx.oft, 60);
                }
                e => panic!("Expected ProtocolIncomplete, got: {:?}", e),
            },
            65 => {
                assert!(rst.unwrap().unwrap().equal(rspstr));
                assert_eq!(stream.len(), 0);
                assert_eq!(stream.ctx, CTX);
            }
            _ => panic!("out of index"),
        }
    }
}
