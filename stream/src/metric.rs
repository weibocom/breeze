use metrics::{Metric, Path};
use protocol::{Operation, OPS};
macro_rules! define_metrics {
    ($($t:ident:$($name:ident),+);+) => {
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
                Self {
                    ops,
                    $(
                        $(
                            $name: path.$t(stringify!($name)),
                        )+
                    )+
                }
            }
        }
    };
}

define_metrics!(qps: tx, rx, err, cps, kps, conn;
count:conn_num;
rtt:avg
);
