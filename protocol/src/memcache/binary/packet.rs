// 用于动态构建请求；
// 同时，效率层面考量，从client接受的请求不解析为该请求结构

use std::{
    io::{self, Cursor, Error, ErrorKind, Result},
    u32, u8,
};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

// mc 二进制协议包，用于构建各种协议指令，所有mc协议构建均需放在这里 fishermen

// cmd的第一个字节，用于标示request or response
pub enum Magic {
    Request = 0x80,
    Response = 0x81,
}

// pub const STATUS_OK: u16 = 0x0;
pub const SET_REQUEST_EXTRATS_LEN: u8 = 8;

// mc 二进制协议头，包括request、response
#[derive(Clone)]
pub struct PacketHeader {
    pub magic: u8,
    pub opcode: u8,
    pub key_length: u16,
    pub extras_length: u8,
    pub data_type: u8,
    // idx为6、7的字节，在request中用于设置vbucket，在response中用于设置status
    pub vbucket_id_or_status: u16,
    pub total_body_length: u32,
    pub opaque: u32,
    pub cas: u64,
}

// set/add/replace 等指令，extra长度为8，flag、exp分别占4个bytes
#[derive(Debug)]
pub struct StoreExtras {
    pub flags: u32,
    pub expiration: u32,
}

// 对于set request，extras len必须是8字节，前4字节是flags，后四字节是expiration
pub struct SetRequest {
    pub header: PacketHeader,
    pub store_extras: StoreExtras,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

// 总共有48个opcode，这里先只部分支持
#[allow(dead_code)]
pub enum Opcode {
    Get = 0x00,
    Set = 0x01,
    Add = 0x02,
    Replace = 0x03,
    Delete = 0x04,
    Increment = 0x05,
    Decrement = 0x06,
    Flush = 0x08,
    Stat = 0x10,
    Noop = 0x0a,
    Version = 0x0b,
    GetKQ = 0x0d,
    SetQ = 0x11,
    Touch = 0x1c,
    StartAuth = 0x21,
}

// mc response的响应code
pub enum Status {
    NoError = 0x0000,
    // NotFound = 0x0001,
}

impl PacketHeader {
    pub fn write<W: io::Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_u8(self.magic)?;
        writer.write_u8(self.opcode)?;
        writer.write_u16::<BigEndian>(self.key_length)?;
        writer.write_u8(self.extras_length)?;
        writer.write_u8(self.data_type)?;
        writer.write_u16::<BigEndian>(self.vbucket_id_or_status)?;
        writer.write_u32::<BigEndian>(self.total_body_length)?;
        writer.write_u32::<BigEndian>(self.opaque)?;
        writer.write_u64::<BigEndian>(self.cas)?;
        return Ok(());
    }

    // 暂时不用，注释掉
    // pub fn read_request<R: io::Read>(reader: &mut R) -> Result<PacketHeader> {
    //     let magic = reader.read_u8()?;
    //     if magic != Magic::Request as u8 {
    //         return Err(Error::new(
    //             ErrorKind::InvalidData,
    //             "request magic is malformed",
    //         ));
    //     }
    //     PacketHeader::read_left(magic, reader)
    // }

    pub fn read_response<R: io::Read>(reader: &mut R) -> Result<PacketHeader> {
        let magic = reader.read_u8()?;
        if magic != Magic::Response as u8 {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "response magic is malformed",
            ));
        }
        PacketHeader::read_left(magic, reader)
    }

    fn read_left<R: io::Read>(magic: u8, reader: &mut R) -> Result<PacketHeader> {
        let header = PacketHeader {
            magic: magic,
            opcode: reader.read_u8()?,
            key_length: reader.read_u16::<BigEndian>()?,
            extras_length: reader.read_u8()?,
            data_type: reader.read_u8()?,
            vbucket_id_or_status: reader.read_u16::<BigEndian>()?,
            total_body_length: reader.read_u32::<BigEndian>()?,
            opaque: reader.read_u32::<BigEndian>()?,
            cas: reader.read_u64::<BigEndian>()?,
        };
        return Ok(header);
    }
}

impl SetRequest {
    pub fn write<W: io::Write>(&self, writer: &mut W) -> Result<()> {
        // set 请求的extras len必须是2
        debug_assert_eq!(self.header.extras_length, SET_REQUEST_EXTRATS_LEN);

        // write header
        self.header.write(writer)?;
        // write extras（flags + expiration）
        writer.write_u32::<BigEndian>(self.store_extras.flags)?;
        writer.write_u32::<BigEndian>(self.store_extras.expiration)?;
        // write key
        writer.write(&self.key)?;
        // write value
        writer.write(&self.value)?;

        Ok(())
    }
}

#[derive(Clone)]
// mc binary ResponsePacket
pub struct ResponsePacket {
    pub header: PacketHeader,
    pub key: Vec<u8>,
    pub extras: Vec<u8>,
    pub value: Vec<u8>,
}

impl ResponsePacket {
    // 暂时不用，注释掉
    // pub(crate) fn err(self) -> Result<Self> {
    //     let status = self.header.vbucket_id_or_status;
    //     if status == STATUS_OK {
    //         Ok(self)
    //     } else {
    //         let err_str = format!("response error code:{}", status);
    //         Err(Error::new(ErrorKind::InvalidData, err_str))?
    //     }
    // }

    pub fn parse_get_response_flag(&self) -> Result<u32> {
        let mut flag = 0;
        if self.extras.len() > 0 {
            flag = Cursor::new(&self.extras).read_u32::<BigEndian>()?;
        }
        Ok(flag)
    }

    pub fn is_ok(&self) -> bool {
        self.header.vbucket_id_or_status == Status::NoError as u16
    }
}

pub fn parse_response_packet<R: io::Read>(reader: &mut R) -> Result<ResponsePacket> {
    let header = PacketHeader::read_response(reader)?;
    let mut extras = vec![0x0; header.extras_length as usize];
    reader.read_exact(extras.as_mut_slice())?;
    let mut key = vec![0x0; header.key_length as usize];
    reader.read_exact(key.as_mut_slice())?;

    // TODO: return error if total_body_length < extras_length + key_length
    let value_len =
        header.total_body_length - header.key_length as u32 - header.extras_length as u32;
    let mut value = vec![0x0; value_len as usize];
    reader.read_exact(value.as_mut_slice())?;
    Ok(ResponsePacket {
        header,
        key,
        extras,
        value,
    })
}
