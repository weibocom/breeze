use ds::RingSlice;

use crate::Operation;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandType {
    VRange = 0,
    VAdd,
    VUpdate,
    VDel,
    Unknown,
}

impl From<RingSlice> for CommandType {
    fn from(name: RingSlice) -> Self {
        let mut oft = 0;
        if name.len() > 7 || name.uppercase_scan(&mut oft) != b'V' {
            return Self::Unknown;
        }

        match name.uppercase_scan(&mut oft) {
            b'R' => Self::to_cmd(&name, "VRANGE", Self::VRange),
            b'A' => Self::to_cmd(&name, "VADD", Self::VAdd),
            b'U' => Self::to_cmd(&name, "VUPDATE", Self::VUpdate),
            b'D' => Self::to_cmd(&name, "VDEL", Self::VDel),
            _ => Self::Unknown,
        }
    }
}

impl CommandType {
    #[inline]
    fn to_cmd(name: &RingSlice, cmd: &str, cmd_type: CommandType) -> Self {
        const CHECKED_LEN: usize = 2;
        if name.len() == cmd.len()
            && name.start_with_case(CHECKED_LEN, cmd[CHECKED_LEN..].as_bytes(), false)
        {
            cmd_type
        } else {
            Self::Unknown
        }
    }

    #[inline]
    pub(super) fn operation(&self) -> Operation {
        match self {
            CommandType::VRange => Operation::Gets,
            CommandType::Unknown => panic!("no operation for unknow!"),
            _ => Operation::Store,
        }
    }
}

impl Default for CommandType {
    fn default() -> Self {
        Self::Unknown
    }
}

/// 扫描对应位置的子节，将对应位置的字符转为大写，同时后移读取位置oft
pub trait Uppercase {
    // 扫描当前子节，转成大写，并讲位置+1
    fn uppercase_scan(&self, oft: &mut usize) -> u8;
}

impl Uppercase for RingSlice {
    fn uppercase_scan(&self, oft: &mut usize) -> u8 {
        let b = self.at(*oft);
        *oft += 1;
        b.to_ascii_uppercase()
    }
}
