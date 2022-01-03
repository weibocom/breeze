// 0~15 bit : op_code
const OP_CODE_BIT: u8 = 16;
// 15~31: 16bit key count
const KEY_COUNT_SHIFT: u8 = 0 + OP_CODE_BIT;
const KEY_COUNT_BITS: u8 = 16;
const KEY_COUNT_MASK: u64 = (1 << KEY_COUNT_BITS) - 1;
// 32: 标识是否是第一个key
const MKEY_FIRST_SHIFT: u8 = KEY_COUNT_SHIFT + KEY_COUNT_BITS;
const MKEY_FIRST_BIT: u8 = 1;
// 33~40: 3bits 是 padding_rsp
const PADDING_RSP_SHIFT: u8 = MKEY_FIRST_SHIFT + MKEY_FIRST_BIT;
const PADDING_RSP_BITS: u8 = 3;
const PADDING_RSP_MASK: u64 = (1 << PADDING_RSP_BITS) - 1;
// 41~48 8bit
const META_LEN_SHIFT: u8 = PADDING_RSP_SHIFT + PADDING_RSP_BITS;
const META_LEN_BITS: u8 = 8;
const META_LEN_MASK: u64 = (1 << META_LEN_BITS) - 1;

const TOKEN_LEN_SHIFT: u8 = META_LEN_BITS + META_LEN_BITS;
const TOKEN_LEN_BITS: u8 = 8;
const TOKEN_LEN_MASK: u64 = (1 << TOKEN_LEN_BITS) - 1;

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

#[inline(always)]
fn set(v: &mut u64, shift: u8, mask: u64, val: u64) {
    debug_assert!(val <= mask);
    debug_assert_eq!(get(v, shift, mask), 0);
    *v |= val << shift;
    debug_assert_eq!(val, get(v, shift, mask));
}
#[inline(always)]
fn get(v: &u64, shift: u8, mask: u64) -> u64 {
    (*v >> shift) & mask
}

impl RedisFlager for u64 {
    #[inline(always)]
    fn set_key_count(&mut self, cnt: u16) {
        set(self, KEY_COUNT_SHIFT, KEY_COUNT_MASK, cnt as u64)
    }
    #[inline(always)]
    fn key_count(&self) -> u16 {
        get(self, KEY_COUNT_SHIFT, KEY_COUNT_MASK) as u16
    }
    #[inline(always)]
    fn set_mkey_first(&mut self) {
        debug_assert!(!self.mkey_first());
        *self |= 1 << MKEY_FIRST_SHIFT;
        debug_assert!(self.mkey_first());
    }
    #[inline(always)]
    fn mkey_first(&self) -> bool {
        *self & (1 << MKEY_FIRST_SHIFT) > 0
    }
    #[inline(always)]
    fn set_padding_rsp(&mut self, padding: u8) {
        set(self, PADDING_RSP_SHIFT, PADDING_RSP_MASK, padding as u64);
    }
    #[inline(always)]
    fn padding_rsp(&self) -> u8 {
        get(self, PADDING_RSP_SHIFT, PADDING_RSP_MASK) as u8
    }
    #[inline(always)]
    fn set_meta_len(&mut self, l: u8) {
        set(self, META_LEN_SHIFT, META_LEN_MASK, l as u64);
    }
    #[inline(always)]
    fn meta_len(&self) -> u8 {
        get(self, META_LEN_SHIFT, META_LEN_MASK) as u8
    }
    #[inline(always)]
    fn set_token_count(&mut self, c: u8) {
        set(self, TOKEN_LEN_SHIFT, TOKEN_LEN_MASK, c as u64);
    }
    #[inline(always)]
    fn token_count(&self) -> u8 {
        get(self, TOKEN_LEN_SHIFT, TOKEN_LEN_MASK) as u8
    }
}
impl RedisFlager for crate::Flag {
    #[inline(always)]
    fn set_key_count(&mut self, cnt: u16) {
        self.ext_mut().set_key_count(cnt);
    }
    #[inline(always)]
    fn key_count(&self) -> u16 {
        self.ext().key_count()
    }
    #[inline(always)]
    fn set_mkey_first(&mut self) {
        self.ext_mut().set_mkey_first();
    }
    #[inline(always)]
    fn mkey_first(&self) -> bool {
        self.ext().mkey_first()
    }
    #[inline(always)]
    fn set_padding_rsp(&mut self, padding: u8) {
        self.ext_mut().set_padding_rsp(padding);
    }
    #[inline(always)]
    fn padding_rsp(&self) -> u8 {
        self.ext().padding_rsp()
    }
    #[inline(always)]
    fn set_meta_len(&mut self, l: u8) {
        self.ext_mut().set_meta_len(l);
    }
    #[inline(always)]
    fn meta_len(&self) -> u8 {
        self.ext().meta_len()
    }
    #[inline(always)]
    fn set_token_count(&mut self, c: u8) {
        self.ext_mut().set_token_count(c);
    }
    #[inline(always)]
    fn token_count(&self) -> u8 {
        self.ext().token_count()
    }
}
