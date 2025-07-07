// 解析mysql协议， 转换为redis协议

use bytes::BytesMut;
use std::fmt::{self, Debug, Display, Formatter};

use crate::kv::client::Client;
use crate::kv::common::constants::StatusFlags;
use crate::kv::common::constants::DEFAULT_MAX_ALLOWED_PACKET;
use crate::kv::common::error::DriverError;
use crate::kv::common::io::ReadMysqlExt;
use crate::kv::common::packets::{AuthPlugin, Column, CommonOkPacket, HandshakeResponse, OkPacket};
use crate::kv::common::proto::MySerialize;
use crate::kv::common::{constants::CapabilityFlags, io::ParseBuf, packets::HandshakePacket};
use crate::kv::common::{proto::codec::PacketCodec, query_result::Or};
use crate::kv::error::Error;
use crate::kv::error::Result;
use crate::kv::HandShakeStatus;
use crate::{Command, StreamContext};
use ds::RingSlice;

use super::packet::MysqlRawPacket;
use super::query_result::{QueryResult, Text};

// const HEADER_LEN: usize = 4;
pub(super) const HEADER_FLAG_OK: u8 = 0x00;
// auth switch 或者 EOF
pub(super) const HEADER_FLAG_CONTINUE: u8 = 0xFE;
// local infile 的response data
pub(super) const HEADER_FLAG_LOCAL_INFILE: u8 = 0xFB;

pub(crate) struct ResponsePacket<'a, S> {
    stream: &'a mut S,
    data: MysqlRawPacket,
    ctx: &'a mut ResponseContext,
    // client: Option<Client>,
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
    pub(super) fn new(stream: &'a mut S) -> Self {
        // let data = stream.slice().into();
        let ctx: &mut ResponseContext = stream.context().into();
        let data = MysqlRawPacket::new(stream.slice(), ctx.status_flags, ctx.capability_flags);
        Self {
            stream,
            data,
            ctx,
            // client,
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

    /// parse mysql response packet，首先解析meta，
    #[inline]
    pub(super) fn parse_result_set(&mut self) -> Result<Command> {
        // 首先parse meta，对于UnhandleResponseError异常，需要构建成响应返回
        let meta = self.parse_result_set_meta()?;

        // 如果是只有meta的ok packet，直接返回影响的列数，如insert/delete/update
        if let Or::B(ok) = meta {
            let affected = ok.affected_rows();
            let mut cmd = self.build_final_affected_rows_rsp_cmd(affected);
            if affected > 0 {
                // 目前affected只对kvector的聚合模式有效，但此处暂时无法区别req，所以统一都设置 fishermen
                cmd.set_count(affected as u32);
            }
            return Ok(cmd);
        }

        // 解析meta后面的rows，并转为redis 格式
        let mut query_result: QueryResult<Text> =
            QueryResult::new(self.data.clone(), self.has_results, meta);
        // 解析出mysql rows
        let redis_data = query_result.parse_rows_to_redis(&mut self.oft)?;

        // 构建响应
        Ok(self.build_final_rsp_cmd(true, redis_data))
    }

    /// 解析mysql的rs meta，如果解析出非incomplete类型的error，说明包解析完毕，需要进行take
    #[inline]
    fn parse_result_set_meta(&mut self) -> Result<Or<Vec<Column>, OkPacket>> {
        // 一个packet全部解析完毕前，不对数据进行take; 反之，必须进行take；
        let payload = self.next_packet()?;

        match payload[0] {
            HEADER_FLAG_OK => {
                let ok = self.data.handle_ok::<CommonOkPacket>(payload)?;
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
        log::debug!("+++ build kvector rsp, ok:{} => {:?}", ok, cmd);

        // 返回最终响应前，take走已经解析的数据
        self.take();
        return cmd;
    }

    /// 构建最终的affected rows 响应，并对已解析的内容进行take
    /// 格式: :num\r\n
    #[inline(always)]
    fn build_final_affected_rows_rsp_cmd(&mut self, n: u64) -> Command {
        // 构建最终返回给client的响应内容
        let r = format!(":{}\r\n", n).into_bytes();
        let mem = ds::MemGuard::from_vec(r);
        let cmd = Command::from(true, mem);
        log::debug!("+++ build vector affected rows rsp, {:?}", cmd);

        // 返回最终响应前，take走已经解析的数据
        self.take();
        return cmd;
    }

    pub(super) fn drop_packet(&mut self) -> Result<()> {
        self.next_packet().map(drop)
    }

    // 尝试读下一个packet的payload，如果数据不完整，返回ProtocolIncomplete
    fn next_packet(&mut self) -> Result<RingSlice> {
        let packet = self.data.next_packet(&mut self.oft)?;
        self.codec.set_seq_id(packet.seq.wrapping_add(1));
        log::debug!("+++ parsed seq:{}", packet.seq);
        Ok(packet.payload)
    }

    // 解析Handshake packet，构建HandshakeResponse packet
    pub(super) fn proc_handshake(&mut self, client: &mut Client) -> crate::Result<()> {
        log::debug!("+++ vector proc_handshake");
        let reply = match self.proc_handshake_inner(client) {
            Ok(r) => r,
            Err(e) => return Err(e.into()),
        };

        self.stream.write(&reply)?;
        Ok(())
    }

    #[inline]
    fn proc_handshake_inner(&mut self, client: &mut Client) -> Result<Vec<u8>> {
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
        self.handle_handshake(&handshake, client);

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
            // .gen_data(self.client.as_ref().unwrap().get_pass(), &*nonce)
            .gen_data(client.get_pass(), &*nonce)
            .as_deref()
            .unwrap_or_default()
            .to_vec();
        let handshake_reply = self.build_handshake_response_packet(
            &auth_plugin,
            RingSlice::from_vec(&auth_data),
            client,
        )?;
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
                self.data.handle_ok::<CommonOkPacket>(payload).map(drop)?;
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

    // 根据auth plugin、scramble及connection 属性，构建shakehandRsp packet
    fn build_handshake_response_packet(
        &mut self,
        auth_plugin: &AuthPlugin,
        // scramble_buf: Option<&[u8]>,
        scramble_buf: RingSlice,
        client: &Client,
    ) -> Result<Vec<u8>> {
        // let client = self.client.as_ref().unwrap();
        let user = client.get_user().unwrap_or_default().as_bytes().to_vec();
        let db_name = client.get_db_name().unwrap_or_default().as_bytes().to_vec();
        let conn_attrs = client.connect_attrs();

        let handshake_response = HandshakeResponse::new(
            Some(scramble_buf),
            client.server_version.unwrap_or((0, 0, 0)),
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
    fn handle_handshake(&mut self, hp: &HandshakePacket, client: &mut Client) {
        log::debug!("+++ vector handle_handshake");
        // let client = self.client.as_mut().unwrap();
        client.capability_flags = hp.capabilities() & client.get_flags();
        client.status_flags = hp.status_flags();
        client.connection_id = hp.connection_id();
        client.character_set = hp.default_collation();
        client.server_version = hp.server_version_parsed();
        // self.mariadb_server_version = hp.maria_db_server_version_parsed();

        // 记录capability_flags、status_flags到stream context
        self.ctx.capability_flags = client.capability_flags;
        self.ctx.status_flags = client.status_flags;
        // *self.stream.context() = self.ctx.into();
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
    pub status_flags: StatusFlags,
    pub capability_flags: CapabilityFlags,
    _ignore: [u8; 8],
}

impl From<&mut StreamContext> for &mut ResponseContext {
    fn from(value: &mut StreamContext) -> Self {
        unsafe { std::mem::transmute(value) }
    }
}

impl From<ResponseContext> for StreamContext {
    fn from(value: ResponseContext) -> Self {
        unsafe { std::mem::transmute(value) }
    }
}
