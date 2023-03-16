// 提供一个满足如下策略的index
// hash / x % y / z
// 其中 x, y, z 都是power of 2

#[derive(Clone, Debug, Default)]
pub struct DivMod {
    x_shift: u8,
    y_mask: u32,
    z_shift: u8,
}

impl DivMod {
    pub fn pow(x: usize, y: usize, z: usize) -> Self {
        debug_assert!(x.is_power_of_two());
        debug_assert!(y.is_power_of_two());
        debug_assert!(z.is_power_of_two());
        debug_assert!(y < u32::MAX as usize);
        let x_shift = x.trailing_zeros() as u8;
        let y_mask = y as u32 - 1;
        let z_shift = z.trailing_zeros() as u8;
        DivMod {
            x_shift,
            y_mask,
            z_shift,
        }
    }

    #[inline(always)]
    pub fn index(&self, hash: i64) -> usize {
        (((hash as usize) >> self.x_shift) & self.y_mask as usize) >> self.z_shift
    }
}
