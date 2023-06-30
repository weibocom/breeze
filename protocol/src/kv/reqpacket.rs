// 解析的mc协议，转换为mysql req 请求
use super::common::{constants::Command, proto::codec::PacketCodec};
use crate::{Error, RequestBuilder, Result};

// mc          vs           mysql
// get                      select
// add                      insert
// set                      update
// del                      delete
// pub(super) const SQL_TYPE_IDX: [u8; 128] = [
//     /*01  2  3  4  5  6  7  8  9  a  b  c  d  e  f  0  1  2  3  4  5  6  7  8  9  a  b  c  d  e  f*/
//     0, 2, 1, 9, 3, 9, 9, 4, 9, 0, 9, 8, 0, 0, 9, 9, 9, 2, 1, 9, 3, 9, 9, 4, 9, 9, 9, 9, 9, 9, 9, 9,
//     9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9,
//     9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9,
//     9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9,
// ];

#[derive(Clone, Debug)]
pub struct RequestPacket {
    codec: PacketCodec,
}

impl RequestPacket {
    pub(super) fn new() -> Self {
        Self {
            codec: Default::default(),
        }
    }

    // TODO 先实现功能，待优化
    pub(super) fn build_request(self, my_cmd: Command, builder: RequestBuilder) -> Result<Vec<u8>> {
        // let mut req_data = BytesMut::with_capacity(DEFAULT_MAX_ALLOWED_PACKET);
        // TODO: 按需分配内存，避免过大，同时需要警惕内存不够导致异常的情况 fishermen
        // let mut req_data = BytesMut::with_capacity(req_len);
        // req_data.put_u8(my_cmd as u8);
        // req_data.put_slice(sql.as_bytes());

        self.codec
            .encode_with_builder(my_cmd, builder)
            .map_err(|e| {
                log::warn!("encode request failed:{:?}", e);
                return Error::WriteResponseErr;
            })
    }
}

impl Default for RequestPacket {
    fn default() -> Self {
        RequestPacket::new()
    }
}

// mysql sql 语句类型
// #[repr(u8)]
// #[derive(PartialEq, Eq, Hash, Clone, Debug)]
// pub(super) enum SqlType {
//     Select = 0,
//     Insert = 1,
//     Update = 2,
//     Delete = 3,
//     Quit = 4,

//     Version = 8,
//     Unknown = 9, //用于区分set、cas
// }

// pub(super) trait TypeConvert {
//     fn sql_type(&self) -> SqlType;
// }

// impl TypeConvert for RingSlice {
//     fn sql_type(&self) -> SqlType {
//         let op_code = self.op() as usize;
//         if op_code >= SQL_TYPE_IDX.len() {
//             log::warn!("found malformed mysql req:{:?}", self);
//             return SqlType::Unknown;
//         }
//         unsafe { std::mem::transmute(SQL_TYPE_IDX[op_code]) }
//     }
// }
