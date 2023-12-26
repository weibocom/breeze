mod command;
pub(crate) mod error;
pub mod flager;
mod query_result;
mod reqpacket;
mod rsppacket;

use crate::{
    Command, Commander, Error, HashedCommand, Metric, MetricItem, Packet, Protocol,
    RequestProcessor, Result, Stream, Writer,
};
use ds::RingSlice;
use sharding::hash::Hash;

use self::flager::KvFlager;
use self::query_result::{QueryResult, Text};
use self::reqpacket::RequestPacket;
use self::rsppacket::ResponsePacket;

use crate::kv::client::Client;
use crate::kv::common::query_result::Or;
use crate::kv::{ContextStatus, HandShakeStatus};
use crate::HandShake;

pub use command::{get_cmd_type, CommandType};

#[derive(Clone, Default)]
pub struct Vector;

impl Protocol for Vector {
    fn parse_request<S: Stream, H: Hash, P: RequestProcessor>(
        &self,
        stream: &mut S,
        alg: &H,
        process: &mut P,
    ) -> Result<()> {
        // 解析redis格式的指令，构建结构化的redis vector 内部cmd，要求：方便转换成sql
        let mut packet = RequestPacket::new(stream);
        match self.parse_request_inner(&mut packet, alg, process) {
            Ok(_) => Ok(()),
            Err(Error::ProtocolIncomplete) => {
                // 如果解析数据不够，提前reserve stream的空间
                packet.reserve_stream_buff();
                Ok(())
            }
            e => {
                log::warn!("redis parsed err: {:?}, req: {:?} ", e, packet.inner_data());
                e
            }
        }
    }

    fn config(&self) -> crate::Config {
        crate::Config {
            need_auth: true,
            ..Default::default()
        }
    }

    fn handshake(
        &self,
        stream: &mut impl Stream,
        option: &mut crate::ResOption,
    ) -> crate::Result<HandShake> {
        log::debug!("+++ vector handshake");
        match self.handshake_inner(stream, option) {
            Ok(h) => Ok(h),
            Err(crate::Error::ProtocolIncomplete) => Ok(HandShake::Continue),
            Err(e) => {
                log::warn!("+++ found err when shake hand:{:?}", e);
                Err(e)
            }
        }
    }

    fn parse_response<S: Stream>(&self, data: &mut S) -> Result<Option<Command>> {
        log::debug!("+++ vector recv mysql response:{:?}", data.slice());
        let mut rsp_packet = ResponsePacket::new(data, None);

        // 解析完毕rsp后，除了数据未读完的场景，其他不管是否遇到err，都要进行take
        match self.parse_response_inner(&mut rsp_packet) {
            Ok(cmd) => Ok(Some(cmd)),
            Err(crate::Error::ProtocolIncomplete) => Ok(None),
            Err(e) => {
                // 非MysqlError需要日志并外层断连处理
                log::error!("+++ err when parse mysql response: {:?}", e);
                Err(e.into())
            }
        }
    }

    fn write_response<C, W, M, I>(
        &self,
        ctx: &mut C,
        response: Option<&mut Command>,
        w: &mut W,
    ) -> crate::Result<()>
    where
        W: Writer,
        C: Commander<M, I>,
        M: Metric<I>,
        I: MetricItem,
    {
        // sendonly 直接返回
        if ctx.request().sentonly() {
            assert!(response.is_none(), "req:{:?}", ctx.request());
            return Ok(());
        }

        if let Some(response) = response {
            log::debug!("+++ send to client {:?} => {:?}", ctx.request(), response);
            if !response.ok() {
                // 对于非ok的响应，需要构建rsp，发给client
                w.write("-ERR ".as_bytes())?;
                w.write_slice(response, 0)?; // mysql返回的错误信息
                w.write("\r\n".as_bytes())?;
                return Ok(());
            } else {
                // response已封装为redis协议。正常响应有三种：
                // 1. 只返回影响的行数
                // 2. 一行或多行数据
                // 3. 结果为空
                w.write_slice(response, 0)?; // value
            }
            return Ok(());
        }

        // 没有响应，考虑quit或返回占位rsp
        let op_code = ctx.request().op_code();
        let cfg = command::get_cfg(op_code).expect("kv cfg");

        // quit 直接返回Err，外层断连接
        if cfg.quit {
            return Err(crate::Error::Quit); // TODO
        }

        use crate::kv::KVCtx;
        let response = match ctx.ctx().ctx().error {
            ContextStatus::TopInvalid => b"-ERR invalid request: year out of index\r\n".as_slice(),
            ContextStatus::ReqInvalid => b"-ERR invalid request\r\n".as_slice(),
            ContextStatus::Ok => cfg.padding_rsp.as_bytes(),
        };

        // 其他场景返回padding rsp
        w.write(response)?;
        log::debug!("+++ send to client padding {:?}", ctx.request());
        Ok(())
    }
}

impl Vector {
    #[inline]
    fn parse_request_inner<S: Stream, H: Hash, P: RequestProcessor>(
        &self,
        packet: &mut RequestPacket<S>,
        alg: &H,
        process: &mut P,
    ) -> Result<()> {
        log::debug!("+++ rec kvector req:{:?}", packet.inner_data());
        while packet.available() {
            packet.parse_bulk_num()?;
            let cfg = packet.parse_cmd_properties()?;
            let mut flag = cfg.flag();
            let key = packet.parse_cmd(cfg, &mut flag)?;

            // 构建cmd，准备后续处理
            let cmd = packet.take();
            let hash = packet.hash(key, alg);
            let cmd = HashedCommand::new(cmd, hash, flag);
            process.process(cmd, true);
        }

        Ok(())
    }

    fn handshake_inner<S: Stream>(
        &self,
        stream: &mut S,
        option: &mut crate::ResOption,
    ) -> crate::Result<HandShake> {
        log::debug!("+++ recv vector handshake packet:{:?}", stream.slice());
        let client = Client::from_user_pwd(option.username.clone(), option.token.clone());
        let mut packet = ResponsePacket::new(stream, Some(client));

        match packet.ctx().status {
            HandShakeStatus::Init => {
                packet.proc_handshake()?;
                packet.ctx().status = HandShakeStatus::InitialhHandshakeResponse;
                Ok(HandShake::Continue)
            }
            HandShakeStatus::InitialhHandshakeResponse => {
                packet.proc_auth()?;
                packet.ctx().status = HandShakeStatus::AuthSucceed;
                log::debug!("+++ proc auth succeed!");
                Ok(HandShake::Success)
            }

            HandShakeStatus::AuthSucceed => Ok(HandShake::Success),
        }
    }

    fn parse_response_inner<'a, S: crate::Stream>(
        &self,
        rsp_packet: &'a mut ResponsePacket<'a, S>,
    ) -> crate::Result<Command> {
        // 首先parse meta，对于UnhandleResponseError异常，需要构建成响应返回
        let meta = match rsp_packet.parse_result_set_meta() {
            Ok(meta) => meta,
            Err(crate::kv::error::Error::UnhandleResponseError(emsg)) => {
                // 对于UnhandleResponseError，需要构建rsp，发给client
                let cmd = rsp_packet.build_final_rsp_cmd(false, emsg);
                return Ok(cmd);
            }
            Err(e) => return Err(e.into()),
        };

        // 如果是只有meta的ok packet，直接返回影响的列数，如insert/delete/update
        if let Or::B(ok) = meta {
            let affected = ok.affected_rows();
            let cmd = rsp_packet.build_final_affected_rows_rsp_cmd(affected);
            return Ok(cmd);
        }

        // 解析meta后面的rows，返回列记录，如select
        // 有可能多行数据，直接build成
        let mut query_result: QueryResult<Text, S> = QueryResult::new(rsp_packet, meta);
        match query_result.parse_rows_to_cmd() {
            Ok(cmd) => Ok(cmd),
            Err(crate::kv::error::Error::UnhandleResponseError(emsg)) => {
                // 对于UnhandleResponseError，需要构建rsp，发给client
                let cmd = query_result.build_final_rsp_cmd(false, emsg);
                Ok(cmd)
            }
            Err(e) => Err(e.into()),
        }
    }
}

// pub struct RingSliceIter {}
// impl Iterator for RingSliceIter {
//     type Item = RingSlice;
//     fn next(&mut self) -> Option<Self::Item> {
//         todo!()
//     }
// }
// pub struct ConditionIter {}
// impl Iterator for ConditionIter {
//     type Item = Condition;
//     fn next(&mut self) -> Option<Self::Item> {
//         todo!()
//     }
// }

pub enum Opcode {}

pub(crate) const COND_ORDER: &[u8] = b"ORDER";
pub(crate) const COND_LIMIT: &[u8] = b"LIMIT";
pub(crate) const COND_GROUP: &[u8] = b"GROUP";

#[derive(Debug, Clone, Default)]
pub struct Condition {
    pub field: RingSlice,
    pub op: RingSlice,
    pub value: RingSlice,
}

impl Condition {
    /// 构建一个condition
    #[inline]
    pub(crate) fn new(field: RingSlice, op: RingSlice, value: RingSlice) -> Self {
        Self { field, op, value }
    }
}

// pub enum Order {
//     ASC,
//     DESC,
// }
#[derive(Debug, Clone, Default)]
pub struct Order {
    pub field: RingSlice,
    pub order: RingSlice,
}
// pub struct Orders {
//     pub field: RingSliceIter,
//     pub order: Order,
// }

#[derive(Debug, Clone, Default)]
pub struct Limit {
    //无需转成int，可直接拼接
    pub offset: RingSlice,
    pub limit: RingSlice,
}
#[derive(Debug, Clone, Default)]
pub struct GroupBy {
    pub fields: RingSlice,
}

pub type Field = (RingSlice, RingSlice);

//非迭代版本，代价是内存申请。如果采取迭代版本，需要重复解析一遍，重复解析可以由parser实现，topo调用
#[derive(Debug, Clone, Default)]
pub struct VectorCmd {
    pub keys: Vec<RingSlice>,
    pub fields: Vec<Field>,
    pub wheres: Vec<Condition>,
    pub order: Order,
    pub limit: Limit,
    pub group_by: GroupBy,
}

/// field 字段的值，对于‘field’关键字，值是｜分隔的field names，否则就是二进制value
#[derive(Debug, Clone)]
pub enum FieldVal {
    Names(Vec<RingSlice>),
    Val(RingSlice),
}

impl FieldVal {
    // TODO 需要在parse时，增加协议判断 fishermen
    #[inline(always)]
    pub fn names(&self) -> &Vec<RingSlice> {
        match self {
            Self::Names(names) => names,
            _ => panic!("mustn't has field bin val"),
        }
    }

    // TODO 需要在parse时，增加协议判断 fishermen
    #[inline(always)]
    pub fn val(&self) -> &RingSlice {
        match self {
            Self::Val(val) => val,
            _ => panic!("mustn't has field names"),
        }
    }

    /// check names，如果names不存在，则返回异常
    #[inline(always)]
    pub fn has_names(&self) -> bool {
        match self {
            Self::Names(_) => true,
            _ => false,
        }
    }

    /// check val，如果val不存在，则返回异常
    pub fn has_val(&self) -> bool {
        match self {
            Self::Val(_) => true,
            _ => false,
        }
    }
}

const FIELD_BYTES: &'static [u8] = b"FIELD";
const KVECTOR_SEPARATOR: u8 = b',';
/// 根据parse的结果，此处进一步获得kvector的detail/具体字段信息，以便进行sql构建
pub fn parse_vector_detail(cmd: &HashedCommand) -> crate::Result<VectorCmd> {
    let data = Packet::from(cmd.sub_slice(0, cmd.len()));
    let flag = cmd.flag();

    let mut vcmd: VectorCmd = Default::default();
    // 解析keys
    parse_vector_key(&data, flag.key_pos() as usize, &mut vcmd)?;

    // 解析fields
    let field_start = flag.field_pos() as usize;
    let condition_pos = flag.condition_pos() as usize;
    let pos_after_field = if condition_pos > 0 {
        const WHERE_LEN: usize = "$5\r\nWHERE\r\n".len();
        condition_pos - WHERE_LEN
    } else {
        data.len()
    };
    parse_vector_field(&data, field_start, pos_after_field, &mut vcmd)?;

    // 解析conditions
    parse_vector_condition(&data, condition_pos, &mut vcmd)?;

    // 校验cmd的合法性
    let cmd_type = get_cmd_type(flag.op_code())?;
    validate_cmd(&vcmd, cmd_type)?;

    Ok(vcmd)
}

#[inline]
fn parse_vector_key(data: &Packet, key_pos: usize, vcmd: &mut VectorCmd) -> Result<()> {
    assert!(key_pos > 0, "{:?}", data);

    // 解析key，format: $-1\r\n or $2\r\nab\r\n
    let mut oft = key_pos;
    let key_data = data.bulk_string(&mut oft)?;
    // let mut keys = Vec::with_capacity(3);
    oft = 0;
    loop {
        if oft >= key_data.len() {
            break;
        }
        // 从keys中split出','分割的key
        let idx = key_data
            .find(oft, KVECTOR_SEPARATOR)
            .unwrap_or(key_data.len());
        vcmd.keys.push(key_data.sub_slice(oft, idx - oft));
        oft = idx + 1;
    }
    Ok(())
}

#[inline]
fn parse_vector_field(
    data: &Packet,
    field_start: usize,
    pos_after_field: usize,
    vcmd: &mut VectorCmd,
) -> Result<()> {
    // field start pos为0，说明没有field
    if field_start == 0 {
        return Ok(());
    }
    let mut oft = field_start;
    while oft < pos_after_field {
        let name = data.bulk_string(&mut oft)?;
        let value = data.bulk_string(&mut oft)?;
        // 对于field关键字，value是field names
        if name.equal_ignore_case(FIELD_BYTES) {
            validate_field_name(value)?;
        } else {
            // 否则 name就是field name
            validate_field_name(name)?;
        };
        vcmd.fields.push((name, value));
    }
    // 解析完fields，结束的位置应该刚好是flag中记录的field之后下一个字节的oft
    assert_eq!(oft, pos_after_field, "packet:{:?}", data);

    Ok(())
}

fn parse_vector_condition(data: &Packet, cond_pos: usize, vcmd: &mut VectorCmd) -> Result<()> {
    // 解析where condition，必须是三段式format: $3\r\nsid\r\n$1\r\n<\r\n$3\r\n100\r\n
    vcmd.wheres = Vec::with_capacity(3);
    if cond_pos > 0 {
        let mut oft = cond_pos;
        while oft < data.len() {
            let name = data.bulk_string(&mut oft)?;
            let op = data.bulk_string(&mut oft)?;
            let val = data.bulk_string(&mut oft)?;
            if name.equal_ignore_case(COND_ORDER) {
                // 先校验 order by 的value/fields
                validate_field_name(val)?;
                vcmd.order = Order {
                    order: op,
                    field: val,
                };
            } else if name.equal_ignore_case(COND_GROUP) {
                // 先校验 group by 的value/fields
                validate_field_name(val)?;
                vcmd.group_by = GroupBy { fields: val }
            } else if name.equal_ignore_case(COND_LIMIT) {
                vcmd.limit = Limit {
                    offset: op,
                    limit: val,
                };
            } else {
                vcmd.wheres.push(Condition::new(name, op, val));
            }
        }
        // 解析完fields，结束的位置应该刚好是data的末尾
        assert_eq!(oft, data.len(), "packet:{:?}", data);
    }
    Ok(())
}

// /// TODO: 反引号方案，目前暂时不用，先注释掉 fishermen
// /// 根据分隔符把field names分拆成多个独立的fields，并对field进行校验；
// /// 校验策略：反引号方案，即ASCII: U+0001 .. U+007F，同时排除0、反引号;
// /// https://dev.mysql.com/doc/refman/8.0/en/identifiers.html
// fn split_validate_field_name(field_names: RingSlice) -> Result<Vec<RingSlice>> {
//     assert!(field_names.len() > 0, "field: {}", field_names);

//     let len = field_names.len();
//     let mut fields = Vec::with_capacity(len / 4 + 1);
//     let mut start = 0;
//     // 校验，并分拆fields
//     for i in 0..len {
//         let c = field_names.at(i);
//         if MYSQL_FIELD_CHAR_TBL[c as usize] == 0 {
//             return Err(crate::Error::RequestProtocolInvalid);
//         }
//         if c == KVECTOR_SEPARATOR {
//             if i > start {
//                 fields.push(field_names.sub_slice(start, i - start));
//             }
//             start = i + 1;
//         }
//     }
//     if (len - 1) > start {
//         fields.push(field_names.sub_slice(start, len - 1 - start));
//     }

//     Ok(fields)
// }

/// 校验fields，不可含有非法字符，避免sql注入
fn validate_field_name(field_name: RingSlice) -> Result<()> {
    assert!(field_name.len() > 0, "field: {}", field_name);

    // 逐字节校验
    for i in 0..field_name.len() {
        let c = field_name.at(i);
        if MYSQL_FIELD_CHAR_TBL[c as usize] == 0 {
            return Err(crate::vector::error::KvectorError::ReqMalformedField.into());
        }
    }

    Ok(())
}

/// 解析cmd detail完毕，开始根据cmd类型进行校验
///   1. vrange: 如果有fields，最多只能有1个，且name为“field”
///   2. vadd：fields必须大于0,不能where condition；
///   3. vupdate： fields必须大于0；
///   4. vdel： fields必须为空；
///   5. vcard：无；
fn validate_cmd(vcmd: &VectorCmd, cmd_type: CommandType) -> Result<()> {
    match cmd_type {
        CommandType::VRange => {
            // vrange 的fields数量不能大于1
            if vcmd.fields.len() > 1
                || (vcmd.fields.len() == 1 && !vcmd.fields[0].0.equal_ignore_case(FIELD_BYTES))
            {
                return Err(crate::Error::RequestInvalidMagic);
            }
        }
        CommandType::VAdd => {
            if vcmd.fields.len() == 0 || vcmd.wheres.len() > 0 {
                return Err(crate::Error::RequestInvalidMagic);
            }
        }
        CommandType::VUpdate => {
            if vcmd.fields.len() == 0 {
                return Err(crate::Error::RequestInvalidMagic);
            }
        }
        CommandType::VDel => {
            if vcmd.fields.len() > 0 {
                return Err(crate::Error::RequestInvalidMagic);
            }
        }
        CommandType::VCard => {}
        _ => {
            panic!("unknown kvector cmd:{:?}", vcmd);
        }
    }
    Ok(())
}

/// mysql 非反引号方案 + 内建函数 + ‘,’，即field中只有如下字符在mesh中是合法的：
///  1. ASCII: [0-9,a-z,A-Z$_] (basic Latin letters, digits 0-9, dollar, underscore)
///  2. 内置函数符号17个：& >= < ( )等
///  3. 永久禁止：‘;’ 和空白符号；
///  具体见 #775
pub static MYSQL_FIELD_CHAR_TBL: [u8; 256] = [
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 1, 0, 0, 1, 1, 1, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 1, 1, 0,
    0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 1, 1,
    0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
];

#[cfg(test)]
mod ValidateTest {
    use super::MYSQL_FIELD_CHAR_TBL;

    fn test_field_tbl() {
        // 校验nums
        let nums = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9'];
        for c in nums {
            assert_eq!(MYSQL_FIELD_CHAR_TBL[c as usize], 1, "nums checked faield");
            println!("{} is ok", c);
        }

        // 校验大写字母
        const A: u8 = b'A';
        const a: u8 = b'a';
        for i in 0..26 {
            let upper = A + i;
            let lower = a + i;
            assert_eq!(
                MYSQL_FIELD_CHAR_TBL[upper as usize], 1,
                "upper alphabets checked faield"
            );
            assert_eq!(
                MYSQL_FIELD_CHAR_TBL[lower as usize], 1,
                "upper alphabets checked faield"
            );
            println!("{} and {} is ok", upper as char, lower as char);
        }

        // 校验特殊字符
        let special_chars = [
            33_u8, 37, 38, 40, 41, 42, 43, 44, 45, 46, 47, 58, 60, 61, 62, 94, 124,
        ];
        for c in special_chars {
            assert_eq!(
                MYSQL_FIELD_CHAR_TBL[c as usize], 1,
                "special checked faield"
            );
            println!("{} is ok", c);
        }

        // 必须禁止的字符
        // let forbidden_chars = [];
    }
}
