use crate::Operation;
#[derive(Debug)]
pub struct Flag {
    v: u64,
}

// 高5字节，归各种协议私用，低3个字节，共享
const RSP_BIT_META_LEN: u8 = 40;
const RSP_BIT_TOKEN_LEN: u8 = 48;
const REQ_BIT_PADDING_RSP: u8 = 32;
const REQ_BIT_MKEY_FIRST: u8 = 24;
// const REQ_BIT_VAL_MKEY_FIRST: u64 = 1 << 24;

impl Flag {
    // first = true 满足所有条件1. 当前请求是multiget；2. 拆分了多个子请求；3. 是`第一`个子请求；
    // last  = true 满足所有条件1. 当前请求是multiget；2. 拆分了多个子请求；3. 是`最后`一个子请求；
    #[inline(always)]
    pub fn from_op(op_code: u8, op: Operation) -> Self {
        let v = ((op_code as u64) << 8) | (op as u64);
        Self { v }
    }

    // **********************  TODO: redis相关，需要按协议分拆 start **********************
    // TODO: 后续flag需要根据协议进行分别构建 speedup fishermen
    #[inline(always)]
    pub fn from_mkey_op(mkey_first: bool, padding_rsp: u8, op: Operation) -> Self {
        let mut v = 0u64;
        // 只有多个key时，first key才有意义
        if mkey_first {
            const MKEY_FIRST_VAL: u64 = 1 << REQ_BIT_MKEY_FIRST;
            v |= MKEY_FIRST_VAL;
        }

        if padding_rsp > 0 {
            v |= (padding_rsp as u64) << REQ_BIT_PADDING_RSP;
        }

        // operation 需要小雨
        v |= op as u64;

        Self { v }
    }

    // meta len 正常不会超过20，256绰绰有余
    pub fn from_metalen_tokencount(metalen: u8, tokencount: u8) -> Self {
        let mut v = 0u64;
        if metalen > 0 {
            v |= (metalen as u64) << RSP_BIT_META_LEN;
        }
        if tokencount > 0 {
            v |= (tokencount as u64) << RSP_BIT_TOKEN_LEN;
        }
        Self { v }
    }

    pub fn padding_rsp(&self) -> u8 {
        const MASK: u64 = (1 << 8) - 1;
        let p = (self.v >> REQ_BIT_PADDING_RSP) & MASK;
        p as u8
    }

    pub fn is_mkey_first(&self) -> bool {
        const MASK_MKF: u64 = 1 << REQ_BIT_MKEY_FIRST;
        (self.v & MASK_MKF) == MASK_MKF
    }

    pub fn meta_len(&self) -> u8 {
        const MASK: u64 = (1 << 8) - 1;
        let len = (self.v >> RSP_BIT_META_LEN) & MASK;
        len as u8
    }

    pub fn token_count(&self) -> u8 {
        const MASK: u64 = (1u64 << 8) - 1;
        let len = (self.v >> RSP_BIT_TOKEN_LEN) & MASK;
        len as u8
    }

    // **********************  TODO: redis相关，需要按协议分拆 end！speedup **********************

    #[inline(always)]
    pub fn new() -> Self {
        Self { v: 0 }
    }
    // 低位第一个字节是operation位
    // 第二个字节是op_code
    const STATUS_OK: u8 = 16;
    const SEND_ONLY: u8 = 17;
    const NO_FORWARD: u8 = 18;

    #[inline(always)]
    pub fn set_status_ok(&mut self) -> &mut Self {
        self.mark(Self::STATUS_OK);
        self
    }
    #[inline(always)]
    pub fn ok(&self) -> bool {
        self.marked(Self::STATUS_OK)
    }
    #[inline(always)]
    pub fn set_sentonly(&mut self) -> &mut Self {
        self.mark(Self::SEND_ONLY);
        self
    }
    #[inline(always)]
    pub fn sentonly(&self) -> bool {
        self.marked(Self::SEND_ONLY)
    }
    #[inline(always)]
    pub fn operation(&self) -> Operation {
        (self.v as u8).into()
    }
    #[inline(always)]
    pub fn op_code(&self) -> u8 {
        // 第二个字节是op_code
        (self.v >> 8) as u8
    }
    #[inline(always)]
    pub fn set_noforward(&mut self) -> &mut Self {
        self.mark(Self::NO_FORWARD);
        self
    }
    #[inline(always)]
    pub fn noforward(&self) -> bool {
        self.marked(Self::NO_FORWARD)
    }

    #[inline(always)]
    pub fn mark(&mut self, bit: u8) {
        self.v |= 1 << bit;
    }
    #[inline(always)]
    pub fn marked(&self, bit: u8) -> bool {
        let m = 1 << bit;
        self.v & m == m
    }
    #[inline(always)]
    pub fn reset_flag(&mut self, op_code: u8, op: Operation) {
        *self = Self::from_op(op_code, op);
    }
}
