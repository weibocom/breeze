// 0~15 bit : op_code
const OP_CODE_BIT: u8 = 16;
// 15~31: 16bit key count
const KEY_COUNT_SHIFT: u8 = 0 + OP_CODE_BIT;
const KEY_COUNT_BITS: u8 = 16;
const KEY_COUNT_MASK: u64 = (1 << KEY_COUNT_BITS) - 1;
// 32: 标识是否是第一个key
const MKEY_FIRST_SHIFT: u8 = KEY_COUNT_SHIFT + KEY_COUNT_BITS;
const MKEY_FIRST_BIT: u8 = 1; // 这个先保留，后续增加字段时需要
                              // 33: master_only
const MASTER_ONLY_SHIFT: u8 = MKEY_FIRST_SHIFT + MKEY_FIRST_BIT;
const MASTER_ONLY_BIT: u8 = 1;
// 34: direct_hash
// const DIRECT_HASH_SHIFT: u8 = MASTER_ONLY_SHIFT + MASTER_ONLY_BIT;
// const _DIRECT_HASH_BIT: u8 = 1;

pub trait RedisFlager {
    fn set_key_count(&mut self, cnt: u16);
    fn key_count(&self) -> u16;
    fn set_mkey_first(&mut self);
    fn mkey_first(&self) -> bool;

    // fn set_padding_rsp(&mut self, idx: u8);
    // fn padding_rsp(&self) -> u8;

    fn set_master_only(&mut self);
    fn master_only(&self) -> bool;
    // fn set_direct_hash(&mut self);
    // fn direct_hash(&self) -> bool;

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

    // TODO 暂时保留，备查及比对，待上线稳定一段时间后再删除，2022.12可删
    // #[inline]
    // fn set_padding_rsp(&mut self, padding: u8) {
    //     self.mask_set(PADDING_RSP_SHIFT, PADDING_RSP_MASK, padding as u64);
    // }
    // #[inline]
    // fn padding_rsp(&self) -> u8 {
    //     self.mask_get(PADDING_RSP_SHIFT, PADDING_RSP_MASK) as u8
    // }

    #[inline]
    fn set_master_only(&mut self) {
        self.set(MASTER_ONLY_SHIFT)
    }
    #[inline]
    fn master_only(&self) -> bool {
        self.get(MASTER_ONLY_SHIFT)
    }
    // #[inline]
    // fn set_direct_hash(&mut self) {
    //     self.set(DIRECT_HASH_SHIFT);
    // }
    // #[inline]
    // fn direct_hash(&self) -> bool {
    //     self.get(DIRECT_HASH_SHIFT)
    // }
}
