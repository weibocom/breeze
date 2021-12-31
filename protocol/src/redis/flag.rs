// 高5字节，归各种协议私用，低3个字节，共享
const RSP_BIT_META_LEN: u8 = 40;
const RSP_BIT_TOKEN_LEN: u8 = 48;
const REQ_BIT_PADDING_RSP: u8 = 32;
const REQ_BIT_MKEY_FIRST: u8 = 24;
// const REQ_BIT_VAL_MKEY_FIRST: u64 = 1 << 24;
//
//

pub(super) trait RedisFlager {
    fn set_key_count(&mut self, cnt: u16);
    fn key_count(&self) -> u16;
    fn set_mkey_first(&mut self);
    fn mkey_first(&self) -> bool;
    fn set_padding_rsp(&mut self, idx: u8);
    fn padding_rsp(&self) -> u8;
    fn set_meta_len(&mut self, l: u8);
    fn meta_len(&self) -> u8;
    fn set_token_count(&mut self, c: u8);
    fn token_count(&self) -> u8;
}

impl RedisFlager for crate::Flag {
    #[inline(always)]
    fn set_key_count(&mut self, cnt: u16) {
        debug_assert_eq!(self.key_count(), 0);
        *self.ext_mut() |= cnt as u64;
    }
    #[inline(always)]
    fn key_count(&self) -> u16 {
        self.ext() as u16
    }
    #[inline(always)]
    fn set_mkey_first(&mut self) {
        *self.ext_mut() |= 1 << REQ_BIT_MKEY_FIRST;
    }
    #[inline(always)]
    fn mkey_first(&self) -> bool {
        self.ext() & (1 << REQ_BIT_MKEY_FIRST) > 0
    }
    #[inline(always)]
    fn set_padding_rsp(&mut self, padding: u8) {
        debug_assert_eq!(self.padding_rsp(), 0);
        *self.ext_mut() |= (padding as u64) << REQ_BIT_PADDING_RSP;
    }
    #[inline(always)]
    fn padding_rsp(&self) -> u8 {
        (self.ext() >> REQ_BIT_PADDING_RSP) as u8
    }
    #[inline(always)]
    fn set_meta_len(&mut self, l: u8) {
        debug_assert_eq!(self.meta_len(), 0);
        *self.ext_mut() |= (l as u64) << RSP_BIT_META_LEN;
    }
    #[inline(always)]
    fn meta_len(&self) -> u8 {
        (self.ext() >> RSP_BIT_META_LEN) as u8
    }
    #[inline(always)]
    fn set_token_count(&mut self, c: u8) {
        debug_assert_eq!(self.token_count(), 0);
        *self.ext_mut() |= (c as u64) << RSP_BIT_TOKEN_LEN;
    }
    #[inline(always)]
    fn token_count(&self) -> u8 {
        (self.ext() >> RSP_BIT_TOKEN_LEN) as u8
    }
}
