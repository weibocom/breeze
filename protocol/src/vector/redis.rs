use super::{command::get_cfg, flager::KvFlager, *};

use crate::{Flag, Packet, Result};
use ds::RingSlice;

pub(crate) const FIELD_BYTES: &'static [u8] = b"FIELD";
pub(crate) const KVECTOR_SEPARATOR: u8 = b',';

/// 根据parse的结果，此处进一步获得kvector的detail/具体字段信息，以便进行sql构建
pub fn parse_vector_detail(
    cmd: RingSlice,
    flag: &Flag,
    config_aggregation: bool,
) -> crate::Result<VectorCmd> {
    let data = Packet::from(cmd);

    let mut vcmd: VectorCmd = Default::default();
    let cmd_props = get_cfg(flag.op_code())?;
    vcmd.cmd = cmd_props.cmd_type;
    // 解析keys
    parse_vector_key(&data, flag.key_pos() as usize, &mut vcmd)?;

    // 只对strategy为aggregation的配置，才设置route，否则直接访问当前main库表
    if config_aggregation {
        vcmd.route = Some(cmd_props.route);
    }

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
    validate_cmd(&vcmd, vcmd.cmd)?;

    Ok(vcmd)
}

#[inline]
pub(crate) fn parse_vector_key(data: &Packet, key_pos: usize, vcmd: &mut VectorCmd) -> Result<()> {
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
pub(crate) fn parse_vector_field(
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

pub(crate) fn parse_vector_condition(
    data: &Packet,
    cond_pos: usize,
    vcmd: &mut VectorCmd,
) -> Result<()> {
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

/// 校验fields，不可含有非法字符，避免sql注入
pub(crate) fn validate_field_name(field_name: RingSlice) -> Result<()> {
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
///   6. vget：key不能为0
pub(crate) fn validate_cmd(vcmd: &VectorCmd, cmd_type: CommandType) -> Result<()> {
    match cmd_type {
        CommandType::VRange | CommandType::VRangeTimeline => {
            // vrange 的fields数量不能大于1
            if vcmd.fields.len() > 1
                || (vcmd.fields.len() == 1 && !vcmd.fields[0].0.equal_ignore_case(FIELD_BYTES))
            {
                return Err(crate::Error::RequestInvalidMagic);
            }
        }
        CommandType::VAdd | CommandType::VAddSi | CommandType::VAddTimeline => {
            if vcmd.fields.len() == 0 || vcmd.wheres.len() > 0 {
                return Err(crate::Error::RequestInvalidMagic);
            }
        }
        CommandType::VUpdate | CommandType::VUpdateTimeline => {
            if vcmd.fields.len() == 0 {
                return Err(crate::Error::RequestInvalidMagic);
            }
        }
        CommandType::VDel | CommandType::VDelSi | CommandType::VDelTimeline => {
            if vcmd.fields.len() > 0 {
                return Err(crate::Error::RequestInvalidMagic);
            }
        }
        CommandType::VGet => {
            // vget 的key不能为0
            if vcmd.keys[0].len() == 1 && vcmd.keys[0][0] == b'0' {
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
mod validate_test {
    use super::MYSQL_FIELD_CHAR_TBL;

    #[test]
    pub fn test_field_tbl() {
        // 校验nums
        let nums = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9'];
        for c in nums {
            assert_eq!(MYSQL_FIELD_CHAR_TBL[c as usize], 1, "nums checked faield");
            println!("{} is ok", c);
        }

        // 校验大写字母
        const A_UPPER: u8 = b'A';
        const A_LOWER: u8 = b'a';
        for i in 0..26 {
            let upper = A_UPPER + i;
            let lower = A_LOWER + i;
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
        let forbidden_bytes = [9_u8, 10, 11, 12, 13, 32, 59];
        for b in forbidden_bytes {
            assert_eq!(
                MYSQL_FIELD_CHAR_TBL[b as usize], 0,
                "{} should forbidden forever!",
                b as char
            );
        }
    }
}
