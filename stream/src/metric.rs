use metrics::{Metric, Path};
use protocol::{Operation, OPS};
macro_rules! define_metrics {
    ($($t:ident:$($name:ident-$key:expr),+);+) => {
        pub struct CbMetrics {
            $(
                $(
                $name: Metric,
                )+
            )+
            ops: [Metric; OPS.len()],
        }
        impl CbMetrics {
            $(
            $(
            #[inline(always)]
            pub fn $name(&mut self) -> &mut Metric {
                &mut self.$name
            }
            )+
            )+
            #[inline(always)]
            pub fn ops(&mut self, op:Operation) -> &mut Metric {
                debug_assert!(op.id() < self.ops.len());
                unsafe{self.ops.get_unchecked_mut(op.id()) }
            }
            pub fn new(path:&Path) -> Self {
                let ops: [Metric; OPS.len()] =
                    array_init::array_init(|idx| path.rtt(OPS[idx].name()));
                   // let rat: [Metric; OPS.len()] =
                    //array_init::array_init(|idx| path.ratio(OPS[idx].name()));
                Self {
                    ops,
                    //rat,
                    $(
                        $(
                            $name: path.$t(stringify!($key)),
                        )+
                    )+
                }
            }


        }
    };
}

define_metrics!(qps: tx-tx, rx-rx, err-err, cps-cps, kps-kps, conn-conn;
count:conn_num-conn;
rtt:avg-avg;
ratio:right-sum
);
