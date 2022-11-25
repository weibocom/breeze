// 0~15 bit : op_code
const OP_CODE_BIT: u8 = 16;
// 15~31: 16bit key count
const KEY_COUNT_SHIFT: u8 = 0 + OP_CODE_BIT;
const KEY_COUNT_BITS: u8 = 16;
const KEY_COUNT_MASK: u64 = (1 << KEY_COUNT_BITS) - 1;
// 32: 标识是否是第一个key
const MKEY_FIRST_SHIFT: u8 = KEY_COUNT_SHIFT + KEY_COUNT_BITS;
const MKEY_FIRST_BIT: u8 = 1;  // 这个先保留，后续增加字段时需要
// 33: master_only
const MASTER_ONLY_SHIFT: u8 = MKEY_FIRST_SHIFT + MKEY_FIRST_BIT;
const MASTER_ONLY_BIT: u8 = 1;
// 34: direct_hash
const DIRECT_HASH_SHIFT: u8 = MASTER_ONLY_SHIFT + MASTER_ONLY_BIT;
const _DIRECT_HASH_BIT: u8 = 1;

// 这些目前不再使用，暂时保留到2022.12 之后清理
// TODO 测试完毕后清理 deadcode
// 33~35: 3bits 是 padding_rsp
// const PADDING_RSP_SHIFT: u8 = MKEY_FIRST_SHIFT + MKEY_FIRST_BIT;
// const PADDING_RSP_BITS: u8 = 3;
// const PADDING_RSP_MASK: u64 = (1 << PADDING_RSP_BITS) - 1;

// // 36~43 8bit
// const META_LEN_SHIFT: u8 = PADDING_RSP_SHIFT + PADDING_RSP_BITS;
// const META_LEN_BITS: u8 = 8;
// const META_LEN_MASK: u64 = (1 << META_LEN_BITS) - 1;

// token len 目前没有用，先注释掉 fishermen
// const TOKEN_LEN_SHIFT: u8 = META_LEN_BITS + META_LEN_BITS;
// const TOKEN_LEN_BITS: u8 = 8;
// const TOKEN_LEN_MASK: u64 = (1 << TOKEN_LEN_BITS) - 1;

pub trait RedisFlager {
    fn set_key_count(&mut self, cnt: u16);
    fn key_count(&self) -> u16;
    fn set_mkey_first(&mut self);
    fn mkey_first(&self) -> bool;
    
    // fn set_padding_rsp(&mut self, idx: u8);
    // fn padding_rsp(&self) -> u8;

    fn set_master_only(&mut self);
    fn master_only(&self) -> bool;
    fn set_direct_hash(&mut self);
    fn direct_hash(&self) -> bool;
    
    // fn set_ignore_rsp(&mut self, ignore_rsp: bool);
    // fn ignore_rs(&self) -> bool;

    // fn set_meta_len(&mut self, l: u8);
    // fn meta_len(&self) -> u8;
    // fn set_token_count(&mut self, c: u8);
    // fn token_count(&self) -> u8;
}

use crate::Bit;
impl RedisFlager for u64 {
    #[inline]
    fn set_key_count(&mut self, cnt: u16) {
        self.mask_set(KEY_COUNT_SHIFT, KEY_COUNT_MASK, cnt as u64)
    }
    #[inline]
    fn key_count(&self) -> u16 {
        self.mask_get(KEY_COUNT_SHIFT, KEY_COUNT_MASK) as u16
    }
    #[inline]
    fn set_mkey_first(&mut self) {
        assert!(!self.mkey_first());
        *self |= 1 << MKEY_FIRST_SHIFT;
        assert!(self.mkey_first());
    }
    #[inline]
    fn mkey_first(&self) -> bool {
        self.get(MKEY_FIRST_SHIFT)
    }

？拉到本地fix
    #[inline]
    fn set_padding_rsp(&mut self, padding: u8) {
        self.mask_set(PADDING_RSP_SHIFT, PADDING_RSP_MASK, padding as u64);
    }
    #[inline]
    fn padding_rsp(&self) -> u8 {
        self.mask_get(PADDING_RSP_SHIFT, PADDING_RSP_MASK) as u8
    }
    
    #[inline]
    fn set_master_only(&mut self) {
        self.set(MASTER_ONLY_SHIFT)
    }
    #[inline]
    fn master_only(&self) -> bool {
        self.get(MASTER_ONLY_SHIFT)
    }
    #[inline]
    fn set_direct_hash(&mut self) {
        self.set(DIRECT_HASH_SHIFT);
    }
    #[inline]
    fn direct_hash(&self) -> bool {
        self.get(DIRECT_HASH_SHIFT)
    }
    
    // #[inline]
    // fn set_meta_len(&mut self, l: u8) {
    //     set(self, META_LEN_SHIFT, META_LEN_MASK, l as u64);
    // }
    // #[inline]
    // fn meta_len(&self) -> u8 {
    //     get(self, META_LEN_SHIFT, META_LEN_MASK) as u8
    // }
    // #[inline]
    // fn set_token_count(&mut self, c: u8) {
    //     set(self, TOKEN_LEN_SHIFT, TOKEN_LEN_MASK, c as u64);
    // }
    // #[inline]
    // fn token_count(&self) -> u8 {
    //     get(self, TOKEN_LEN_SHIFT, TOKEN_LEN_MASK) as u8
    // }
}
