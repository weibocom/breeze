//use crate::ToRatio;
use crate::{Id, ItemWriter, NumberInner};
pub struct Ratio {
    deno: NumberInner,  //分母,keyqps
    mole: NumberInner,  //请求key
    ratio: NumberInner, //   比例
}
impl Ratio {
    // 只计数。
    #[inline(always)]
    pub(crate) fn snapshot<W: ItemWriter>(&self, id: &Id, w: &mut W, secs: f64) {
        let (molelast, molecur) = self.mole.load_and_snapshot();
        let mole = molecur - molelast;
        // if mole > 0 {
        // w.write(&id.path, id.key, "ratio_count", mole as f64 / secs);
        let (denolast, denocur) = self.deno.load_and_snapshot();
        let deno = denocur - denolast;
        if deno > 0 {
            w.write(&id.path, id.key, "ratio", deno as f64 / mole as f64);
        }
    }

    // #[inline(always)]

    // pub(crate) fn incr(&self, d: ToRatio) {
    //     self.count.incr(1);
    //     let us = d.as_micros() as i64;
    //     self.avg_us.incr(us);
    // }
}
