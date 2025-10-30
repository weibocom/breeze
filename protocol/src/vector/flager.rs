/// 存放kv的解析字段&位置信息，kv协议: $cmd $key [$field_name $field_value]* [where [$condtion $op $condvar]+]*
/// $cmd、$key是必须的，所以只需要记录$field 位置，where condition语句的位置即可；为了加速，也记录key的位置。

/// [0..8] key
const KEY_POS_SHIFT: u8 = 0;
const KEY_POS_BITS: u8 = 8;
const KEY_POS_MASK: u64 = (1 << KEY_POS_BITS) - 1;
// /// [8..16]field_count
// const FIELD_COUNT_SHIFT: u8 = KEY_POS_SHIFT + KEY_POS_BITS;
// const FIELD_COUNT_BITS: u8 = 8;
// const FIELD_COUNT_MASK: u64 = (1 << FIELD_COUNT_BITS) - 1;
/// [8..16] field_pos
const FIELD_POS_SHIFT: u8 = KEY_POS_SHIFT + KEY_POS_BITS;
const FIELD_POS_BITS: u8 = 8;
const FIELD_POS_MASK: u64 = (1 << FIELD_POS_BITS) - 1;
// /// [16..24] condtion count，注意condition是三段式
// const CONDITION_COUNT_SHIFT: u8 = FIELD_POS_SHIFT + FIELD_POS_BITS;
// const CONDITION_COUNT_BITS: u8 = 8;
// const CONDITION_COUNT_MASK: u64 = (1 << CONDITION_COUNT_MASK) - 1;
/// [16-40] condition pos
const CONDITION_POS_SHIFT: u8 = FIELD_POS_SHIFT + FIELD_POS_BITS;
pub(super) const CONDITION_POS_BITS: u8 = 24;
const CONDITION_POS_MASK: u64 = (1 << CONDITION_POS_BITS) - 1;

// [40]: 标识是否是第一个key
const MKEY_FIRST_SHIFT: u8 = CONDITION_POS_SHIFT + CONDITION_POS_BITS;
// const MKEY_FIRST_BIT: u8 = 1; // 这个先保留，后续增加字段时需要
/// bits:41-63 暂时保留
/// 备注：当前kv最多支持的指令名称长度不超过200，field pair不超过200对，condition数量不超过200段;
///       如果field pos、condition pos超过u16，返回u16::max,具体位置需要调用方再次扫描获取;

pub trait KvFlager {
    fn set_key_pos(&mut self, key_pos: u8);
    fn key_pos(&self) -> u8;
    // fn set_field_count(&mut self, field_count: u8);
    // fn field_count(&mut self) -> u8;
    fn set_field_pos(&mut self, field_pos: u8);
    fn field_pos(&self) -> u8;
    // fn set_condition_count(&mut self, condition_count: u8);
    // fn condition_count(&self) -> u8;
    fn set_condition_pos(&mut self, condition_pos: u32);
    fn condition_pos(&self) -> u32;
    fn set_mkey_first(&mut self);
    fn mkey_first(&self) -> bool;
}

use crate::{Bit, Ext};
impl<T: Ext> KvFlager for T {
    #[inline]
    fn set_key_pos(&mut self, key_pos: u8) {
        assert!(key_pos < u8::MAX);
        self.mask_set(KEY_POS_SHIFT, KEY_POS_MASK, key_pos as u64);
    }
    #[inline]
    fn key_pos(&self) -> u8 {
        self.mask_get(KEY_POS_SHIFT, KEY_POS_MASK) as u8
    }
    // #[inline]
    // fn set_field_count(&mut self, field_count: u8) {
    //     self.mask_set(FIELD_COUNT_SHIFT, FIELD_COUNT_MASK, field_count as u64);
    // }
    // #[inline]
    // fn field_count(&mut self) -> u8 {
    //     self.mask_get(FIELD_COUNT_SHIFT, FIELD_COUNT_MASK);
    // }
    #[inline]
    fn set_field_pos(&mut self, field_pos: u8) {
        assert!(field_pos < u8::MAX);
        self.mask_set(FIELD_POS_SHIFT, FIELD_POS_MASK, field_pos as u64);
    }
    #[inline]
    fn field_pos(&self) -> u8 {
        self.mask_get(FIELD_POS_SHIFT, FIELD_POS_MASK) as u8
    }
    // #[inline]
    // fn set_condition_count(&mut self, condition_count: u8) {
    //     self.mask_set(
    //         CONDITION_COUNT_SHIFT,
    //         CONDITION_COUNT_MASK,
    //         condition_count as u64,
    //     );
    // }
    // #[inline]
    // fn condition_count(&self) -> u8 {
    //     self.mask_get(CONDITION_COUNT_SHIFT, CONDITION_COUNT_MASK) as u8
    // }
    /// 设置condition pos，3个子节以内
    #[inline]
    fn set_condition_pos(&mut self, condition_pos: u32) {
        assert!((condition_pos as u64) < CONDITION_POS_MASK);

        self.mask_set(
            CONDITION_POS_SHIFT,
            CONDITION_POS_MASK,
            condition_pos as u64,
        );
    }
    #[inline]
    fn condition_pos(&self) -> u32 {
        self.mask_get(CONDITION_POS_SHIFT, CONDITION_POS_MASK) as u32
    }
    #[inline]
    fn set_mkey_first(&mut self) {
        debug_assert!(!self.mkey_first());
        self.set(MKEY_FIRST_SHIFT);
        //*self.ext_mut() |= 1 << MKEY_FIRST_SHIFT;
        debug_assert!(self.mkey_first());
    }
    #[inline]
    fn mkey_first(&self) -> bool {
        self.get(MKEY_FIRST_SHIFT)
    }
}
