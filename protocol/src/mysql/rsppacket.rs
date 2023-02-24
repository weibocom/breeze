// TODO 解析mysql协议， 转换为mc vs redis 协议

use crate::ResOption;

use super::HandShakeStatus;

use byteorder::{ByteOrder, LittleEndian};
use core::num::NonZeroUsize;
use std::collections::HashMap;
use std::fmt::{self, Debug, Display, Formatter};
use std::process;

use super::constants::StatusFlags;
use super::error::DriverError;
use super::opts::Opts;
use super::packets::{
    AuthPlugin, CommonOkPacket, HandshakeResponse, OkPacket, OkPacketDeserializer, OkPacketKind,
};
use super::{constants::CapabilityFlags, io::ParseBuf, packets::HandshakePacket};
use ds::{MemGuard, RingSlice};

use crate::mysql::error::Error::MySqlError;
use crate::mysql::proto::MySerialize;
use crate::{mysql::packets::ErrPacket, HashedCommand, Result, Stream};
use crate::{Command, Error, Flag};

pub(super) struct ResponsePacket<'a, S> {
    stream: &'a mut S,
    data: RingSlice,

    ctx: &'a mut ResponseContext,

    // packet的起始3字节
    payload_len: usize,
    // packet的第四个字节
    seq_id: u8,
    // TODO：这些需要整合到connection中，handshake 中获取的字段
    capability_flags: CapabilityFlags,
    connection_id: u32,
    status_flags: StatusFlags,
    character_set: u8,

    // TODO：这些需要整合到connection中 fishermen
    opts: Opts,
    last_command: u8,
    connected: bool,
    has_results: bool,
    server_version: Option<(u16, u16, u16)>,
    /// Last Ok packet, if any.
    ok_packet: Option<OkPacket<'static>>,

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
            payload_len: 0,
            seq_id: 0,
            capability_flags: Default::default(),
            connection_id: Default::default(),
            status_flags: Default::default(),
            character_set: Default::default(),
            opts,
            last_command: Default::default(),
            connected: Default::default(),
            has_results: Default::default(),
            server_version: Default::default(),
            ok_packet: None,

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

    // 读一个完整的响应包，如果数据不完整，返回ProtocolIncomplete
    pub(super) fn next_packet(&mut self) -> Result<()> {
        // mysql packet至少需要4个字节来读取sequence id
        if self.data.len() < 4 {
            return Err(Error::ProtocolIncomplete);
        }
        let mut data: Vec<u8> = Vec::with_capacity(4);
        self.data.copy_to_vec(&mut data);
        let raw_chunk_len = LittleEndian::read_u24(&data) as usize;
        self.payload_len = raw_chunk_len;
        let seq_id = self.data.at(3);
        self.oft += 3;

        match NonZeroUsize::new(raw_chunk_len) {
            Some(_chunk_len) => {
                // TODO 此处暂时不考虑max_allowed packet问题，由分配内存的位置考虑? fishermen
                self.seq_id = seq_id;
            }
            None => {
                return Err(Error::ProtocolIncomplete);
            }
        };

        // TODO 4 字节之后是各种实际的packet
        const PREFIX_LEN: usize = 4;
        if self.data.len() >= PREFIX_LEN + raw_chunk_len {
            // 0xFF ERR packet header
            if self.current() == 0xFF {
                let mut data: Vec<u8> = Vec::with_capacity(raw_chunk_len);
                self.data
                    .sub_slice(PREFIX_LEN, raw_chunk_len)
                    .copy_to_vec(&mut data);
                match ParseBuf(&data[0..]).parse(self.capability_flags)? {
                    // TODO Error process 异常响应稍后处理 fishermen
                    ErrPacket::Error(server_error) => {
                        // self.handle_err();
                        return Err(MySqlError(From::from(server_error)).error());
                    }
                    ErrPacket::Progress(_progress_report) => {
                        return Err(DriverError::UnexpectedPacket.error());
                    }
                }
            }

            log::warn!("mysql sucess rsp:{:?}", self.data);
            return Ok(());
        }
        log::warn!("incomplte rsp:{:?}", self.data);
        Err(Error::ProtocolIncomplete)
    }

    // 解析Handshake packet，构建HandshakeResponse packet
    pub(super) fn proc_handshake(&mut self) -> Result<HashedCommand> {
        // 先读一个完整的packet
        self.next_packet()?;

        // 解析整个packet
        let mut handshake_data = Vec::with_capacity(self.payload_len);
        self.data.copy_to_vec(&mut handshake_data);
        // TODO 先跑通，后面统一基于RingSlice进行parse
        self.take();
        let handshake = ParseBuf(&handshake_data[0..]).parse::<HandshakePacket>(())?;

        // 3.21.0 之后handshake是v10版本，更古老的版本不支持
        if handshake.protocol_version() != 10u8 {
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

        // 处理nonce，即scramble的2个部分
        // Handshake scramble is always 21 bytes length (20 + zero terminator)
        // scramble_1 8bytes，scramble_2最多13bytes
        let nonce = {
            let mut nonce = Vec::from(handshake.scramble_1_ref());
            nonce.extend_from_slice(handshake.scramble_2_ref().unwrap_or(&[][..]));
            // Trim zero terminator. Fill with zeroes if nonce
            // is somehow smaller than 20 bytes (this matches the server behavior).
            nonce.resize(20, 0);
            nonce
        };

        // 处理auth_plugin，默认使用NativePassword
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
        let mem_guard = MemGuard::from_vec(handshake_rsp);

        // TODO 先随意设置一个op_code
        let op_code = 0_u8;
        let mut flag = Flag::from_op(op_code as u16, crate::Operation::Meta);
        flag.set_try_next_type(crate::TryNextType::NotTryNext);
        flag.set_sentonly(false);
        flag.set_noforward(false);
        let cmd = HashedCommand::new(mem_guard, 0, flag);
        Ok(cmd)
    }

    // 处理handshakeresponse的mysql响应，即auth
    pub(super) fn proc_auth(&mut self) -> Result<()> {
        // 先读取一个packet
        self.next_packet()?;

        // Ok packet header 是 0x00 或者0xFE
        match self.current() {
            0x00 => {
                log::info!("found ok packet for mysql");
                self.handle_ok::<CommonOkPacket>().map(drop);
                return Ok(());
            }
            0xFE => {
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

    // 根据auth plugin、scramble及connection 属性，构建shakehandRsp packet
    fn build_handshake_response_packet(
        &self,
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
        Ok(buf)
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
        let mut client_flags = CapabilityFlags::CLIENT_PROTOCOL_41
            // | CapabilityFlags::CLIENT_SECURE_CONNECTION
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

    fn handle_ok<T: OkPacketKind>(&mut self) -> crate::Result<()> {
        // TODO 先跑通，后面统一基于RingSlice进行parse
        let mut packet_data: Vec<u8> = Vec::with_capacity(self.payload_len);
        self.data.copy_to_vec(&mut packet_data);
        self.take();

        let ok = ParseBuf(&packet_data[0..])
            .parse::<OkPacketDeserializer<T>>(self.capability_flags)?
            .into_inner();
        self.status_flags = ok.status_flags();
        self.ok_packet = Some(ok.clone().into_owned());
        // TODO 暂时先不返回ok packet
        Ok(())
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
        let packet = self.parse_packet()?;
        let packet: InitialHandshake = packet.parse();
        //为了take走还有效
        let packet = packet.clone();
        self.take();
        Ok(packet)
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
