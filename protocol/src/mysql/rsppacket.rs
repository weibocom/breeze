// TODO 解析mysql协议， 转换为mc vs redis 协议

use crate::mysql::constants::DEFAULT_MAX_ALLOWED_PACKET;
use crate::ResOption;

use super::buffer_pool::Buffer;
use super::proto::codec::PacketCodec;
use super::query_result::Or;
use super::HandShakeStatus;

use byteorder::{ByteOrder, LittleEndian};
use bytes::BytesMut;
use core::num::NonZeroUsize;
use std::collections::HashMap;
use std::fmt::{self, Debug, Display, Formatter};
use std::process;

use super::constants::{StatusFlags, MAX_PAYLOAD_LEN};
use super::error::DriverError;
use super::opts::Opts;
use super::packets::{
    AuthPlugin, Column, CommonOkPacket, HandshakeResponse, OkPacket, OkPacketDeserializer,
    OkPacketKind, OldEofPacket, ResultSetTerminator,
};
use super::{constants::CapabilityFlags, io::ParseBuf, packets::HandshakePacket};
use ds::RingSlice;

use crate::mysql::error::Error::MySqlError;
use crate::mysql::io::ReadMysqlExt;
use crate::mysql::proto::MySerialize;
use crate::{mysql::packets::ErrPacket, Result};
use crate::{Command, Error};

const HEADER_LEN: usize = 4;
pub(super) const HEADER_FLAG_OK: u8 = 0x00;
// auth switch 或者 EOF
pub(super) const HEADER_FLAG_CONTINUE: u8 = 0xFE;
// local infile 的response data
pub(super) const HEADER_FLAG_LOCAL_INFILE: u8 = 0xFB;

pub(super) struct ResponsePacket<'a, S> {
    stream: &'a mut S,
    data: RingSlice,

    ctx: &'a mut ResponseContext,

    codec: PacketCodec,

    // packet的起始3字节
    payload_len: usize,
    // packet的第四个字节
    // seq_id: u8,
    // TODO：这些需要整合到connection中，handshake 中获取的字段
    capability_flags: CapabilityFlags,
    connection_id: u32,
    status_flags: StatusFlags,
    character_set: u8,

    // TODO：这些需要整合到connection中 fishermen
    opts: Opts,
    // last_command: u8,
    // connected: bool,
    has_results: bool,
    server_version: Option<(u16, u16, u16)>,
    /// Last Ok packet, if any.
    // ok_packet: Option<OkPacket<'static>>,
    oft_last: usize,
    oft: usize,
}

impl<'a, S: crate::Stream> ResponsePacket<'a, S> {
    pub(super) fn new(stream: &'a mut S, opts_op: Option<Opts>) -> Self {
        let data = stream.slice();
        let opts = match opts_op {
            Some(opt) => opt,
            None => Default::default(),
        };
        let ctx = stream.context().into();
        Self {
            stream,
            data,
            ctx,
            codec: PacketCodec::default(),
            payload_len: 0,
            // seq_id: 0,
            capability_flags: Default::default(),
            connection_id: Default::default(),
            status_flags: Default::default(),
            character_set: Default::default(),
            opts,
            // last_command: Default::default(),
            // connected: Default::default(),
            has_results: Default::default(),
            server_version: Default::default(),
            // ok_packet: None,
            oft_last: 0,
            oft: 0,
        }
    }

    pub(super) fn available(&self) -> bool {
        self.oft < self.data.len()
    }

    #[inline]
    fn current(&self) -> u8 {
        assert!(self.available(), "mysql:{:?}", self.data);
        self.data.at(self.oft)
    }

    pub(super) fn parse_result_set_meta(&mut self) -> Result<Or<Vec<Column>, OkPacket<'static>>> {
        log::debug!("+++ in handle result set");
        // 全部解析完毕前，不对数据进行take
        let payload = self.next_packet_data(false)?;
        log::debug!("+++ column hdr packet data:{:?}", payload);
        match payload[0] {
            HEADER_FLAG_OK => {
                log::debug!("+++ parsed ok rs data:{:?}", payload);
                let ok = self.handle_ok::<CommonOkPacket>(&payload)?;
                self.take();
                Ok(Or::B(ok.into_owned()))
            }
            HEADER_FLAG_LOCAL_INFILE => {
                assert!(false, "not support local infile now!");
                self.take();
                Err(Error::ProtocolNotSupported)
            }
            _ => {
                let mut reader = &payload[..];
                let column_count = reader.read_lenenc_int()?;
                log::debug!("+++ parsed result set column count:{:?}", column_count);
                let mut columns: Vec<Column> = Vec::with_capacity(column_count as usize);
                for i in 0..column_count {
                    let pld = self.next_packet_data(false)?;
                    let column = ParseBuf(&*pld).parse(())?;
                    log::debug!("+++ parsed column-{} :{:?}", i, pld);
                    columns.push(column);
                }
                // skip eof packet
                log::debug!("+++ will drop eof ");
                self.drop_packet()?;
                self.has_results = column_count > 0;
                log::debug!(
                    "+++ parsed columns succeed! left data:{:?}, columns: {:?}",
                    self.data.sub_slice(self.oft, self.left_len()),
                    columns
                );
                Ok(Or::A(columns))
            }
        }
    }

    /// TODO 解析一个完整的packet，并copy出待解析的数据，注意copy只是临时动作，待优化  fishermen
    pub(super) fn next_packet_data(&mut self, take_data: bool) -> Result<Vec<u8>> {
        // 先确认解析出一个完整的packet
        self.next_packet()?;

        // TODO：将对应的payload copy出，供类型转换使用，后续这一步骤需要优化掉 fishermen
        let mut payload = Vec::with_capacity(self.payload_len);
        self.data
            .sub_slice(self.oft, self.payload_len)
            .copy_to_vec(&mut payload);

        // 对stream中已经copy出来的数据进行take掉
        self.oft += self.payload_len;
        if take_data {
            self.take();
        }

        Ok(payload)
    }

    /// Must not be called before handle_handshake.
    const fn has_capability(&self, flag: CapabilityFlags) -> bool {
        self.capability_flags.contains(flag)
    }

    // TODO 先在此处take，后续需要改为最后解析完毕后，只take一次  fishermen
    pub(super) fn next_row_packet(&mut self) -> Result<Option<Buffer>> {
        if !self.has_results {
            return Ok(None);
        }

        log::debug!("+++ will parse next row...");
        // TODO 这个可以通过row count来确定最后一行记录，然后进行一次性take fishermen
        let pld = self.next_packet_data(false)?;
        log::debug!("+++ read next row data succeed!");

        if self.has_capability(CapabilityFlags::CLIENT_DEPRECATE_EOF) {
            if pld[0] == 0xfe && pld.len() < MAX_PAYLOAD_LEN {
                self.has_results = false;
                self.handle_ok::<ResultSetTerminator>(&pld)?;
                return Ok(None);
            }
        } else {
            // TODO 根据mysql doc，EOF_Packet的长度是小于9
            if pld[0] == 0xfe && pld.len() < 8 {
                self.has_results = false;
                self.handle_ok::<OldEofPacket>(&pld)?;
                return Ok(None);
            }
        }

        log::debug!("+++ parsed next row succeed!");
        let buff = Buffer::new(pld);
        Ok(Some(buff))
    }

    pub(super) fn drop_packet(&mut self) -> Result<()> {
        self.next_packet_data(false).map(drop)
    }

    #[inline(always)]
    fn copy_left_to_vec(&mut self, data: &mut Vec<u8>) {
        self.data
            .sub_slice(self.oft, self.left_len())
            .copy_to_vec(data);
    }

    #[inline(always)]
    fn left_len(&self) -> usize {
        self.data.len() - self.oft
    }

    // 读一个完整的响应包，如果数据不完整，返回ProtocolIncomplete
    fn next_packet(&mut self) -> Result<()> {
        // mysql packet至少需要4个字节来读取sequence id
        if self.data.len() <= HEADER_LEN {
            return Err(Error::ProtocolIncomplete);
        }

        // 解析mysql packet header
        let mut data: Vec<u8> = Vec::with_capacity(HEADER_LEN);
        self.copy_left_to_vec(&mut data);
        let raw_chunk_len = LittleEndian::read_u24(&data) as usize;
        self.payload_len = raw_chunk_len;
        let seq_id = data[3];
        self.oft += HEADER_LEN;

        match NonZeroUsize::new(raw_chunk_len) {
            Some(_chunk_len) => {
                // TODO 此处暂时不考虑max_allowed packet问题，由分配内存的位置考虑? fishermen
                self.codec.set_seq_id(seq_id.wrapping_add(1));
            }
            None => {
                return Err(Error::ProtocolIncomplete);
            }
        };

        // 4 字节之后是各种实际的packet payload
        if data.len() >= HEADER_LEN + raw_chunk_len {
            // 0xFF ERR packet header
            if self.current() == 0xFF {
                match ParseBuf(&data[HEADER_LEN..]).parse(self.capability_flags)? {
                    // TODO Error process 异常响应稍后处理 fishermen
                    ErrPacket::Error(server_error) => {
                        // self.handle_err();
                        log::warn!("+++ parse packet err:{:?}", server_error);
                        return Err(MySqlError(From::from(server_error)).error());
                    }
                    ErrPacket::Progress(_progress_report) => {
                        log::warn!("+++ parse packet Progress err:{:?}", _progress_report);
                        return Err(DriverError::UnexpectedPacket.error());
                    }
                }
            }

            // self.seq_id = self.seq_id.wrapping_add(1);
            log::warn!("mysql sucess rsp:{:?}", data);
            return Ok(());
        }

        Err(Error::ProtocolIncomplete)
    }

    // 解析Handshake packet，构建HandshakeResponse packet
    pub(super) fn proc_handshake(&mut self) -> Result<Vec<u8>> {
        // 读取完整packet，并解析为HandshakePacket
        let packet_data = self.next_packet_data(true)?;
        let handshake = ParseBuf(&packet_data[0..]).parse::<HandshakePacket>(())?;

        log::debug!("+++ mysql capabilities:{:?}", handshake.capabilities());

        // 3.21.0 之后handshake是v10版本，不支持更古老的版本
        if handshake.protocol_version() != 10u8 {
            log::warn!("unsupport mysql proto version should be 10");
            return Err(DriverError::UnsupportedProtocol(handshake.protocol_version()).error());
        }

        if !handshake
            .capabilities()
            .contains(CapabilityFlags::CLIENT_PROTOCOL_41)
        {
            return Err(DriverError::Protocol41NotSet.error());
        }

        self.handle_handshake(&handshake);

        // TODO 当前先不支持ssl，后续再考虑 fishermen

        // 处理nonce，即scramble的2个部分:scramble_1 8bytes，scramble_2最多13bytes
        // Handshake scramble is always 21 bytes length (20 + zero terminator)
        let nonce = handshake.nonce();

        // 获取auth_plugin，默认使用NativePassword
        let auth_plugin = handshake
            .auth_plugin()
            .unwrap_or(AuthPlugin::MysqlNativePassword);
        if let AuthPlugin::Other(ref name) = auth_plugin {
            let plugin_name = String::from_utf8_lossy(name).into();
            return Err(DriverError::UnknownAuthPlugin(plugin_name).error());
        }

        let auth_data = auth_plugin.gen_data(self.opts.get_pass(), &*nonce);

        let handshake_rsp =
            self.build_handshake_response_packet(&auth_plugin, auth_data.as_deref())?;

        Ok(handshake_rsp)
    }

    // 处理handshakeresponse的mysql响应，默认是ok即auth
    pub(super) fn proc_auth(&mut self) -> Result<()> {
        // 先读取一个OK/Err packet
        let payload = self.next_packet_data(true)?;

        // Ok packet header 是 0x00 或者0xFE
        match payload[0] {
            HEADER_FLAG_OK => {
                log::info!("found ok packet for mysql");
                self.handle_ok::<CommonOkPacket>(&payload).map(drop)?;
                return Ok(());
            }
            HEADER_FLAG_CONTINUE => {
                // TODO 稍后支持auth switch fishermen
                log::warn!("unsupport auth_switched now");
                return Err(DriverError::UnexpectedPacket.error());
            }
            _ => return Err(DriverError::UnexpectedPacket.error()),
        }
    }

    // TODO speed up
    pub(super) fn proc_cmd(&mut self) -> Result<Option<Command>> {
        Ok(None)
    }

    pub(super) fn more_results_exists(&self) -> bool {
        self.status_flags
            .contains(StatusFlags::SERVER_MORE_RESULTS_EXISTS)
    }

    // 根据auth plugin、scramble及connection 属性，构建shakehandRsp packet
    fn build_handshake_response_packet(
        &mut self,
        auth_plugin: &AuthPlugin<'_>,
        scramble_buf: Option<&[u8]>,
    ) -> Result<Vec<u8>> {
        let handshake_response = HandshakeResponse::new(
            scramble_buf,
            self.server_version.unwrap_or((0, 0, 0)),
            self.opts.get_user().map(str::as_bytes),
            self.opts.get_db_name().map(str::as_bytes),
            Some(auth_plugin.clone()),
            self.capability_flags,
            Some(self.connect_attrs().clone()),
        );
        let mut buf: Vec<u8> = Vec::with_capacity(256);
        handshake_response.serialize(&mut buf);
        let mut src_buf = BytesMut::with_capacity(buf.len());
        src_buf.extend(buf);

        let mut encoded_raw = BytesMut::with_capacity(DEFAULT_MAX_ALLOWED_PACKET);
        match self.codec.encode(&mut src_buf, &mut encoded_raw) {
            Ok(_) => {
                let mut encoded = Vec::with_capacity(encoded_raw.len());
                encoded.extend(&encoded_raw[0..]);
                return Ok(encoded);
            }
            Err(e) => {
                log::warn!("encode request failed:{:?}", e);
                return Err(Error::WriteResponseErr);
            }
        }
    }

    // TODO ======= handle_handshake、get_client_flags 都需要移到conn关联的逻辑中 fishermen
    fn handle_handshake(&mut self, hp: &HandshakePacket<'_>) {
        self.capability_flags = hp.capabilities() & self.get_client_flags();
        self.status_flags = hp.status_flags();
        self.connection_id = hp.connection_id();
        self.character_set = hp.default_collation();
        self.server_version = hp.server_version_parsed();
        // self.mariadb_server_version = hp.maria_db_server_version_parsed();
    }

    fn get_client_flags(&self) -> CapabilityFlags {
        let client_flags = CapabilityFlags::CLIENT_PROTOCOL_41
            | CapabilityFlags::CLIENT_SECURE_CONNECTION
            | CapabilityFlags::CLIENT_LONG_PASSWORD
            | CapabilityFlags::CLIENT_TRANSACTIONS
            | CapabilityFlags::CLIENT_LOCAL_FILES
            | CapabilityFlags::CLIENT_MULTI_STATEMENTS
            | CapabilityFlags::CLIENT_MULTI_RESULTS
            | CapabilityFlags::CLIENT_PS_MULTI_RESULTS
            | CapabilityFlags::CLIENT_PLUGIN_AUTH
            | CapabilityFlags::CLIENT_CONNECT_ATTRS
            | (self.capability_flags & CapabilityFlags::CLIENT_LONG_FLAG);
        // if self.0.opts.get_compress().is_some() {
        //     client_flags.insert(CapabilityFlags::CLIENT_COMPRESS);
        // }

        // TODO 默认dbname 需要从config获取 fishermen
        // if let Some(db_name) = self.opts.get_db_name() {
        //     if !db_name.is_empty() {
        //         client_flags.insert(CapabilityFlags::CLIENT_CONNECT_WITH_DB);
        //     }
        // }

        // TODO 暂时不支持ssl fishermen
        // if self.is_insecure() && self.0.opts.get_ssl_opts().is_some() {
        //     client_flags.insert(CapabilityFlags::CLIENT_SSL);
        // }

        client_flags | self.opts.get_additional_capabilities()
    }

    fn connect_attrs(&self) -> HashMap<String, String> {
        let program_name = match self.opts.get_connect_attrs().get("program_name") {
            Some(program_name) => program_name.clone(),
            None => {
                let arg0 = std::env::args_os().next();
                let arg0 = arg0.as_ref().map(|x| x.to_string_lossy());
                arg0.unwrap_or_else(|| "".into()).to_owned().to_string()
            }
        };

        let mut attrs = HashMap::new();

        attrs.insert("_client_name".into(), "mesh-mysql".into());
        attrs.insert("_client_version".into(), env!("CARGO_PKG_VERSION").into());
        // attrs.insert("_os".into(), env!("CARGO_CFG_TARGET_OS").into());
        attrs.insert("_pid".into(), process::id().to_string());
        // attrs.insert("_platform".into(), env!("CARGO_CFG_TARGET_ARCH").into());
        attrs.insert("program_name".into(), program_name);

        for (name, value) in self.opts.get_connect_attrs().clone() {
            attrs.insert(name, value);
        }

        attrs
    }

    pub(super) fn handle_ok<'t, T: OkPacketKind>(
        &mut self,
        payload: &'t Vec<u8>,
    ) -> crate::Result<OkPacket<'t>> {
        let ok = ParseBuf(payload)
            .parse::<OkPacketDeserializer<T>>(self.capability_flags)?
            .into_inner();
        self.status_flags = ok.status_flags();
        log::debug!("+++ mysql ok packet:{:?}", ok);
        Ok(ok)
    }

    #[inline]
    pub(super) fn take(&mut self) -> ds::MemGuard {
        assert!(self.oft_last < self.oft, "packet:{}", self.data);
        let data = self.data.sub_slice(self.oft_last, self.oft - self.oft_last);
        self.oft_last = self.oft;
        self.stream.take(data.len())
    }

    //解析initial_handshake，暂定解析成功会take走stream，协议未完成返回incomplete
    //如果这样会copy，可返回引用，外部take，但问题不大
    pub(super) fn take_initial_handshake(&mut self) -> Result<InitialHandshake> {
        // let packet = self.parse_packet()?;
        // let packet: InitialHandshake = packet.parse();
        // //为了take走还有效
        // let packet = packet.clone();
        // self.take();
        // Ok(packet)
        todo!()
    }
    //构建采用Native Authentication快速认证的handshake response，seq+1
    pub(super) fn build_handshake_response(
        &mut self,
        option: &ResOption,
        auth_data: &[u8],
    ) -> Result<Vec<u8>> {
        todo!()
    }
    //take走一个packet，如果是err packet 返回错误类型，set+1
    pub(super) fn take_and_ok(&mut self) -> Result<()> {
        todo!();
    }

    pub(super) fn ctx(&mut self) -> &mut ResponseContext {
        self.ctx
    }

    pub(crate) fn parse_packet(&mut self) -> Result<RingSlice> {
        todo!()
    }

    #[inline]
    pub(super) fn reserve(&mut self) {
        if self.oft > self.data.len() {
            self.stream.reserve(self.oft)
        }
    }
}

impl<'a, S: crate::Stream> Display for ResponsePacket<'a, S> {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "(packet => len:{} oft:({} => {})) data:{:?}",
            self.data.len(),
            self.oft_last,
            self.oft,
            self.data
        )
    }
}
impl<'a, S: crate::Stream> Debug for ResponsePacket<'a, S> {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(self, f)
    }
}

#[derive(Clone)]
pub(super) struct InitialHandshake {
    pub(super) _auth_plugin_name: String,
    pub(super) auth_plugin_data: String,
}

impl InitialHandshake {
    pub(super) fn check_fast_auth_and_native(&self) -> Result<()> {
        Ok(())
    }
}

// 这个context用于多请求之间的状态协作
// 必须是u64长度的。
#[repr(C)]
#[derive(Debug)]
pub(super) struct ResponseContext {
    pub(super) seq_id: u8,
    pub(super) status: HandShakeStatus,
    _ignore: [u8; 7],
}

impl From<&mut u64> for &mut ResponseContext {
    fn from(value: &mut u64) -> Self {
        unsafe { std::mem::transmute(value) }
    }
}

//原地反序列化,
pub(super) trait ParsePacket<T> {
    fn parse(&self) -> T;
}

impl<T> ParsePacket<T> for RingSlice {
    fn parse(&self) -> T {
        todo!();
    }
}

// TODO 代码冲突，merge 到上面的ResponsePacket，暂时保留备查 fishermen
// //解析rsp的时候，take的时候seq才加一？
// pub(super) struct ResponsePacket<'a, S> {
//     stream: &'a mut S,
//     ctx: &'a mut ResponseContext,
// }
// impl<'a, S: crate::Stream> ResponsePacket<'a, S> {
//     #[inline]
//     pub(super) fn new(stream: &'a mut S) -> Self {
//         //from实现解除了ctx和stream的关联 ，所以可以有两个mut引用
//         let ctx = stream.context().into();
//         Self { stream, ctx }
//     }
//     pub(super) fn ctx(&mut self) -> &mut ResponseContext {
//         self.ctx
//     }

//     pub(crate) fn parse_packet(&mut self) -> Result<RingSlice> {
//         todo!()
//     }

//     #[inline]
//     pub(crate) fn take(&mut self) -> ds::MemGuard {
//         todo!()
//     }

//     //解析initial_handshake，暂定解析成功会take走stream，协议未完成返回incomplete
//     //如果这样会copy，可返回引用，外部take，但问题不大
//     pub(super) fn take_initial_handshake(&mut self) -> Result<InitialHandshake> {
//         let packet = self.parse_packet()?;
//         let packet: InitialHandshake = packet.parse();
//         //为了take走还有效
//         let packet = packet.clone();
//         self.take();
//         Ok(packet)
//     }
//     //构建采用Native Authentication快速认证的handshake response，seq+1
//     pub(super) fn build_handshake_response(
//         &mut self,
//         option: &ResOption,
//         auth_data: &[u8],
//     ) -> Result<Vec<u8>> {
//         todo!()
//     }
//     //take走一个packet，如果是err packet 返回错误类型，set+1
//     pub(super) fn take_and_ok(&mut self) -> Result<()> {
//         todo!();
//     }
// }