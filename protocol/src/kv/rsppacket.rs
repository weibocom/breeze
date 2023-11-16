// 解析mysql协议， 转换为mc协议

use crate::kv::common::constants::DEFAULT_MAX_ALLOWED_PACKET;
use crate::{Command, StreamContext};

use super::client::Client;
use super::common::{buffer_pool::Buffer, proto::codec::PacketCodec, query_result::Or};

use super::error::Error;
use super::error::Result;
use super::packet::PacketData;
use super::HandShakeStatus;

use bytes::BytesMut;
use core::num::NonZeroUsize;
use std::fmt::{self, Debug, Display, Formatter};

use super::common::constants::{StatusFlags, MAX_PAYLOAD_LEN};
use super::common::error::DriverError;
use super::common::packets::{
    AuthPlugin, Column, CommonOkPacket, HandshakeResponse, OkPacket, OkPacketDeserializer,
    OkPacketKind, OldEofPacket, ResultSetTerminator,
};
use super::common::{constants::CapabilityFlags, io::ParseBuf, packets::HandshakePacket};
use ds::RingSlice;

use crate::kv::common::error::Error::MySqlError;
use crate::kv::common::io::ReadMysqlExt;
use crate::kv::common::packets::ErrPacket;
use crate::kv::common::proto::MySerialize;

// const HEADER_LEN: usize = 4;
pub(super) const HEADER_FLAG_OK: u8 = 0x00;
// auth switch 或者 EOF
pub(super) const HEADER_FLAG_CONTINUE: u8 = 0xFE;
// local infile 的response data
pub(super) const HEADER_FLAG_LOCAL_INFILE: u8 = 0xFB;

pub(crate) struct ResponsePacket<'a, S> {
    stream: &'a mut S,
    data: PacketData,
    ctx: &'a mut ResponseContext,
    client: Option<Client>,

    codec: PacketCodec,
    has_results: bool,
    oft: usize,
    oft_last: usize,
    // data: RingSlice,
    // packet的起始3字节，基于ringSlice后，不需要payload len字段
    // payload_len: usize,
    // packet的第四个字节
    // seq_id: u8,
    // last_command: u8,
    // connected: bool,
    // Last Ok packet, if any.
    // ok_packet: Option<OkPacket<'static>>,
    // 目前只用一次性take，去掉oft_last，测试完毕后清理 fishermen

    // oft_packet: usize, //每个packet开始的oft
}

impl<'a, S: crate::Stream> ResponsePacket<'a, S> {
    pub(super) fn new(stream: &'a mut S, client: Option<Client>) -> Self {
        let data = stream.slice().into();
        let ctx = stream.context().into();
        Self {
            stream,
            data,
            ctx,
            client,
            codec: PacketCodec::default(),
            has_results: Default::default(),
            oft: 0,
            oft_last: 0,
            // payload_len: 0,
            // seq_id: 0,
            // last_command: Default::default(),
            // connected: Default::default(),
            // ok_packet: None,

            // oft_packet: 0,
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

    /// 解析mysql的rs meta，如果解析出非incomplete类型的error，说明包解析完毕，需要进行take
    #[inline]
    pub(super) fn parse_result_set_meta(&mut self) -> Result<Or<Vec<Column>, OkPacket>> {
        // 一个packet全部解析完毕前，不对数据进行take; 反之，必须进行take；
        let payload = self.next_packet()?;

        match payload[0] {
            HEADER_FLAG_OK => {
                let ok = self.handle_ok::<CommonOkPacket>(payload)?;
                // self.take();
                Ok(Or::B(ok.into_owned()))
            }
            HEADER_FLAG_LOCAL_INFILE => {
                assert!(false, "not support local infile now!");
                // self.take();
                // Err(Error::ProtocolNotSupported)
                panic!("unsupport infile req/rsp:{:?}", payload)
            }
            _ => {
                // let mut reader = &payload[..];
                let mut reader = ParseBuf::from(payload);
                let column_count = reader.read_lenenc_int()?;
                let mut columns: Vec<Column> = Vec::with_capacity(column_count as usize);
                for _i in 0..column_count {
                    let pld = self.next_packet()?;
                    let column = ParseBuf::from(pld).parse(())?;
                    columns.push(column);
                }
                // skip eof packet
                self.drop_packet()?;
                self.has_results = column_count > 0;
                Ok(Or::A(columns))
            }
        }
    }

    /// 构建最终的响应，并对已解析的内容进行take
    #[inline(always)]
    pub(super) fn build_final_rsp_cmd(&mut self, ok: bool, rsp_data: Vec<u8>) -> Command {
        // 构建最终返回给client的响应内容
        let mem = ds::MemGuard::from_vec(rsp_data);
        let cmd = Command::from(ok, mem);
        log::debug!("+++ build mysql rsp, ok:{} => {:?}", ok, cmd);

        // 返回最终响应前，take走已经解析的数据
        self.take();
        return cmd;
    }

    /// Must not be called before handle_handshake.
    const fn has_capability(&self, flag: CapabilityFlags) -> bool {
        if let Some(client) = &self.client {
            client.capability_flags.contains(flag)
        } else {
            false
        }
    }

    pub(super) fn next_row_packet(&mut self) -> Result<Option<Buffer>> {
        if !self.has_results {
            return Ok(None);
        }

        let pld = self.next_packet()?;

        if self.has_capability(CapabilityFlags::CLIENT_DEPRECATE_EOF) {
            if pld[0] == 0xfe && pld.len() < MAX_PAYLOAD_LEN {
                self.has_results = false;
                self.handle_ok::<ResultSetTerminator>(pld)?;
                return Ok(None);
            }
        } else {
            // 根据mysql doc，EOF_Packet的长度是小于9?
            if pld[0] == 0xfe && pld.len() < 8 {
                self.has_results = false;
                self.handle_ok::<OldEofPacket>(pld)?;
                return Ok(None);
            }
        }

        let buff = Buffer::new(pld);
        Ok(Some(buff))
    }

    pub(super) fn drop_packet(&mut self) -> Result<()> {
        self.next_packet().map(drop)
    }

    fn capability_flags(&self) -> CapabilityFlags {
        if let Some(client) = &self.client {
            client.capability_flags
        } else {
            CapabilityFlags::empty()
        }
    }

    // /// 读取下一个packet的payload，处理逻辑：
    // ///     1 遇到数据不够，直接返回ProtocolIncomplete
    // ///     2 遇到其他异常，先take掉err data，再返回Err
    // ///     3 正常情况下，返回payload，等后续全部处理后，再统一take
    // fn _next_packet(&mut self) -> Result<RingSlice> {
    //     match self.try_next_packet() {
    //         Ok(pld) => Ok(pld),
    //         Err(Error::ProtocolIncomplete) => Err(crate::Error::ProtocolIncomplete),
    //         Err(e) => {
    //             // 发现异常，说明异常数据已读完，此处统一take
    //             self.take();
    //             Err(e)
    //         }
    //     }
    // }

    // 尝试读下一个packet的payload，如果数据不完整，返回ProtocolIncomplete
    fn next_packet(&mut self) -> Result<RingSlice> {
        // self.oft_packet = self.oft;
        // self.payload_len = header.payload_len;

        let header = self.data.parse_header(&mut self.oft)?;
        match NonZeroUsize::new(header.payload_len) {
            Some(_chunk_len) => {
                // 当前请求基于com query方式，不需要seq id fishermen
                self.codec.set_seq_id(header.seq.wrapping_add(1));
            }
            None => {
                // 当前mysql协议，不应该存在payload长度为0的packet fishermen
                let emsg: Vec<u8> = "zero len response".as_bytes().to_vec();
                log::error!("malformed mysql rsp: {}/{}", self.oft, self.data);
                return Err(Error::UnhandleResponseError(emsg));
            }
        };

        // 4 字节之后是packet 的 body/payload 以及其他的packet
        let left_len = self.data.left_len(self.oft);
        if left_len >= header.payload_len {
            // 0xFF ERR packet header
            if self.current() == 0xFF {
                // self.oft += header.payload_len;
                match ParseBuf::from(self.data.sub_slice(self.oft, left_len))
                    .parse(self.capability_flags())?
                {
                    // server返回的异常响应转为MysqlError，对于client request，最终传给client，不用断连接；
                    // 对于连接初期的auth，则直接断连接即可；
                    ErrPacket::Error(server_error) => {
                        // self.handle_err();
                        self.oft += header.payload_len;
                        log::warn!("+++ parse packet err:{:?}", server_error);
                        return Err(MySqlError(From::from(server_error)).error());
                    }
                    ErrPacket::Progress(_progress_report) => {
                        self.oft += header.payload_len;
                        log::error!("+++ parse packet Progress err:{:?}", _progress_report);
                        // 暂时先不支持诸如processlist、session等指令，所以不应该遇到progress report 包 fishermen
                        //return self.next_packet();
                        panic!("unsupport progress report: {}/{:?}", self.oft, self.data);
                    }
                }
            }

            // self.seq_id = self.seq_id.wrapping_add(1);

            let payload_start = self.oft;
            self.oft += header.payload_len;

            return Ok(self.data.sub_slice(payload_start, header.payload_len));
        }

        // 数据没有读完，reserve可读取空间，返回Incomplete异常
        self.reserve();
        Err(Error::ProtocolIncomplete)
    }

    // 解析Handshake packet，构建HandshakeResponse packet
    pub(super) fn proc_handshake(&mut self) -> crate::Result<()> {
        let reply = match self.proc_handshake_inner() {
            Ok(r) => r,
            Err(e) => return Err(e.into()),
        };

        self.stream.write(&reply)?;
        Ok(())
    }

    #[inline]
    fn proc_handshake_inner(&mut self) -> Result<Vec<u8>> {
        // 读取完整packet，并解析为HandshakePacket
        let payload = self.next_packet()?;
        // handshake 只有一个packet，所以读完后可以立即take
        self.take();
        let handshake = ParseBuf::from(payload).parse::<HandshakePacket>(())?;

        // 3.21.0 之后handshake是v10版本，不支持更古老的版本
        if handshake.protocol_version() != 10u8 {
            log::warn!("unsupport mysql proto version should be 10");
            return Err(
                DriverError::UnsupportedProtocol(handshake.protocol_version())
                    .error()
                    .into(),
            );
        }

        if !handshake
            .capabilities()
            .contains(CapabilityFlags::CLIENT_PROTOCOL_41)
        {
            return Err(DriverError::Protocol41NotSet.error().into());
        }

        // 目前不支持ssl fishermen
        self.handle_handshake(&handshake);

        // 处理nonce，即scramble的2个部分:scramble_1 8bytes，scramble_2最多13bytes
        // Handshake scramble is always 21 bytes length (20 + zero terminator)
        let nonce = handshake.nonce();

        // 获取auth_plugin，默认使用NativePassword
        let auth_plugin = handshake
            .auth_plugin()
            .unwrap_or(AuthPlugin::MysqlNativePassword);
        if let AuthPlugin::Other(ref name) = auth_plugin {
            // let plugin_name = String::from_utf8_lossy(name).into();

            debug_assert!(name.len() < 256, "auth plugin name too long: {:?}", name);
            let plugin_name = name.as_string_lossy();
            return Err(DriverError::UnknownAuthPlugin(plugin_name).error().into());
        }

        // let auth_data = auth_plugin.gen_data(self.opts.get_pass(), &*nonce);
        let auth_data = auth_plugin
            .gen_data(self.client.as_ref().unwrap().get_pass(), &*nonce)
            .as_deref()
            .unwrap_or_default()
            .to_vec();
        let handshake_reply = self
            .build_handshake_response_packet(&auth_plugin, Some(RingSlice::from_vec(&auth_data)))?;
        Ok(handshake_reply)
    }

    /// 处理handshakeresponse的mysql响应，默认是ok即auth
    #[inline]
    pub(super) fn proc_auth(&mut self) -> crate::Result<()> {
        match self.proc_auth_inner() {
            Ok(_) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    /// parse并check auth 包，注意数据不进行take
    #[inline]
    fn proc_auth_inner(&mut self) -> Result<()> {
        // 先读取一个OK/Err packet
        let payload = self.next_packet()?;
        // auth 只有一个回包，拿到后可以立即take
        self.take();

        // Ok packet header 是 0x00 或者0xFE
        match payload[0] {
            HEADER_FLAG_OK => {
                log::debug!("found ok packet for mysql");
                self.handle_ok::<CommonOkPacket>(payload).map(drop)?;
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

    pub(super) fn more_results_exists(&self) -> bool {
        if let Some(ref client) = self.client {
            client
                .status_flags
                .contains(StatusFlags::SERVER_MORE_RESULTS_EXISTS)
        } else {
            false
        }
    }

    // 根据auth plugin、scramble及connection 属性，构建shakehandRsp packet
    fn build_handshake_response_packet(
        &mut self,
        auth_plugin: &AuthPlugin,
        // scramble_buf: Option<&[u8]>,
        scramble_buf: Option<RingSlice>,
    ) -> Result<Vec<u8>> {
        let client = self.client.as_ref().unwrap();
        let user = client.get_user().unwrap_or_default().as_bytes().to_vec();
        let db_name = client.get_db_name().unwrap_or_default().as_bytes().to_vec();
        let conn_attrs = client.connect_attrs();

        let handshake_response = HandshakeResponse::new(
            scramble_buf,
            client.server_version.unwrap_or((0, 0, 0)),
            // self.opts.get_user().map(str::as_bytes),
            // self.opts.get_db_name().map(str::as_bytes),
            Some(RingSlice::from_vec(&user)),
            Some(RingSlice::from_vec(&db_name)),
            Some(auth_plugin.clone()),
            client.capability_flags,
            Some(&conn_attrs),
        );
        log::debug!("+++ kv handshake rsp: {}", handshake_response);
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
            Err(_e) => {
                let emsg = format!("encode request failed:{:?}", _e);
                log::warn!("{}", emsg);
                return Err(Error::AuthInvalid(emsg.into()));
            }
        }
    }

    // pub(super) fn handle_ok<T: OkPacketKind>(
    fn handle_handshake(&mut self, hp: &HandshakePacket) {
        let client = self.client.as_mut().unwrap();
        client.capability_flags = hp.capabilities() & client.get_flags();
        client.status_flags = hp.status_flags();
        client.connection_id = hp.connection_id();
        client.character_set = hp.default_collation();
        client.server_version = hp.server_version_parsed();
        // self.mariadb_server_version = hp.maria_db_server_version_parsed();
    }

    pub(super) fn handle_ok<T: OkPacketKind>(
        &mut self,
        payload: RingSlice,
    ) -> super::error::Result<OkPacket> {
        let ok = ParseBuf::from(payload)
            .parse::<OkPacketDeserializer<T>>(self.capability_flags())?
            .into_inner();
        // self.status_flags = ok.status_flags();
        Ok(ok)
    }

    #[inline]
    pub(super) fn take(&mut self) -> ds::MemGuard {
        // 暂时保留，测试完毕后清理 2023.8.20 fishermen
        assert!(self.oft_last < self.oft, "rsp_packet:{:?}", self);
        let len = self.oft - self.oft_last;
        self.oft_last = self.oft;

        self.stream.take(len)
    }

    #[inline(always)]
    pub(super) fn ctx(&mut self) -> &mut ResponseContext {
        self.ctx
    }

    #[inline]
    pub(super) fn reserve(&mut self) {
        if self.oft > self.stream.len() {
            self.stream.reserve(self.oft - self.stream.len())
        }
    }
}

impl<'a, S: crate::Stream> Display for ResponsePacket<'a, S> {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "(packet => len:{} oft:{}/{}) data:{:?}",
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

// 这个context用于多请求之间的状态协作
#[repr(C)]
#[derive(Debug)]
pub(super) struct ResponseContext {
    pub(super) seq_id: u8,
    pub(super) status: HandShakeStatus,
    _ignore: [u8; 14],
}

impl From<&mut StreamContext> for &mut ResponseContext {
    fn from(value: &mut StreamContext) -> Self {
        unsafe { std::mem::transmute(value) }
    }
}
