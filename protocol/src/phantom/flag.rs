// 0~15 bit : op_code
const OP_CODE_BIT: u8 = 16;
// 15~31: 16bit key count
const KEY_COUNT_SHIFT: u8 = 0 + OP_CODE_BIT;
const KEY_COUNT_BITS: u8 = 16;
const KEY_COUNT_MASK: u64 = (1 << KEY_COUNT_BITS) - 1;
// 32: 标识是否是第一个key
const MKEY_FIRST_SHIFT: u8 = KEY_COUNT_SHIFT + KEY_COUNT_BITS;
const _MKEY_FIRST_BIT: u8 = 1;

pub(super) trait RedisFlager {
    fn set_key_count(&mut self, cnt: u16);
    fn key_count(&self) -> u16;
    fn set_mkey_first(&mut self);
    fn mkey_first(&self) -> bool;
    // fn set_padding_rsp(&mut self, idx: u8);
    // fn padding_rsp(&self) -> u8;
}

#[inline]
fn set(v: &mut u64, shift: u8, mask: u64, val: u64) {
    assert!(val <= mask);
    assert_eq!(get(v, shift, mask), 0);
    *v |= val << shift;
    assert_eq!(val, get(v, shift, mask));
}
#[inline]
fn get(v: &u64, shift: u8, mask: u64) -> u64 {
    (*v >> shift) & mask
}

impl RedisFlager for u64 {
    #[inline]
    fn set_key_count(&mut self, cnt: u16) {
        set(self, KEY_COUNT_SHIFT, KEY_COUNT_MASK, cnt as u64)
    }
    #[inline]
    fn key_count(&self) -> u16 {
        get(self, KEY_COUNT_SHIFT, KEY_COUNT_MASK) as u16
    }
    #[inline]
    fn set_mkey_first(&mut self) {
        assert!(!self.mkey_first());
        *self |= 1 << MKEY_FIRST_SHIFT;
        assert!(self.mkey_first());
    }
    #[inline]
    fn mkey_first(&self) -> bool {
        *self & (1 << MKEY_FIRST_SHIFT) > 0
    }
    // #[inline]
    // fn set_padding_rsp(&mut self, padding: u8) {
    //     set(self, PADDING_RSP_SHIFT, PADDING_RSP_MASK, padding as u64);
    // }
    // #[inline]
    // fn padding_rsp(&self) -> u8 {
    //     get(self, PADDING_RSP_SHIFT, PADDING_RSP_MASK) as u8
    // }
}
impl RedisFlager for crate::Flag {
    #[inline]
    fn set_key_count(&mut self, cnt: u16) {
        self.ext_mut().set_key_count(cnt);
    }
    #[inline]
    fn key_count(&self) -> u16 {
        self.ext().key_count()
    }
    #[inline]
    fn set_mkey_first(&mut self) {
        self.ext_mut().set_mkey_first();
    }
    #[inline]
    fn mkey_first(&self) -> bool {
        self.ext().mkey_first()
    }
    // #[inline]
    // fn set_padding_rsp(&mut self, padding: u8) {
    //     self.ext_mut().set_padding_rsp(padding);
    // }
    // #[inline]
    // fn padding_rsp(&self) -> u8 {
    //     self.ext().padding_rsp()
    // }
}
