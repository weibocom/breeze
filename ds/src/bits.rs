pub trait Ext {
    fn ext(&self) -> u64;
    fn ext_mut(&mut self) -> &mut u64;
}

pub trait Bit {
    fn mask_set(&mut self, shift: u8, mask: u64, val: u64);
    fn mask_get(&self, shift: u8, mask: u64) -> u64;
    fn set(&mut self, shift: u8);
    fn clear(&mut self, shift: u8);
    fn get(&self, shift: u8) -> bool;
}

impl<T: Ext> Bit for T {
    //mask决定val中要set的位数
    #[inline]
    fn mask_set(&mut self, shift: u8, mask: u64, val: u64) {
        debug_assert!(val <= mask);
        debug_assert!(shift as u32 + mask.trailing_ones() <= u64::BITS);
        debug_assert_eq!(self.mask_get(shift, mask), 0);
        *self.ext_mut() |= val << shift;
        debug_assert_eq!(val, self.mask_get(shift, mask));
    }
    #[inline]
    fn mask_get(&self, shift: u8, mask: u64) -> u64 {
        debug_assert!(shift as u32 + mask.trailing_ones() <= u64::BITS);
        (self.ext() >> shift) & mask
    }
    #[inline]
    fn set(&mut self, shift: u8) {
        debug_assert!(shift as u32 <= u64::BITS);
        *self.ext_mut() |= 1 << shift;
    }
    #[inline]
    fn clear(&mut self, shift: u8) {
        debug_assert!(shift as u32 <= u64::BITS);
        *self.ext_mut() &= !(1 << (shift));
    }
    #[inline]
    fn get(&self, shift: u8) -> bool {
        debug_assert!(shift as u32 <= u64::BITS);
        self.ext() & (1 << shift) != 0
    }
}

impl Ext for u64 {
    #[inline]
    fn ext(&self) -> u64 {
        *self
    }
    #[inline]
    fn ext_mut(&mut self) -> &mut u64 {
        self
    }
}
