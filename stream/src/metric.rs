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
            #[inline]
            pub fn $name(&mut self) -> &mut Metric {
                &mut self.$name
            }
            )+
            )+
            #[inline]
            pub fn ops(&mut self, op:Operation) -> &mut Metric {
                assert!(op.id() < self.ops.len());
                unsafe{self.ops.get_unchecked_mut(op.id()) }
            }
            pub fn new(path:&Path) -> Self {
                let ops: [Metric; OPS.len()] =
                    array_init::array_init(|idx| path.rtt(OPS[idx].name()));
                Self {
                    ops,
                    $(
                        $(
                            $name: path.$t(stringify!($key)),
                        )+
                    )+
                }
            }
            // debug only
            #[inline]
            pub fn biz(&self) -> &Metric {
                &self.cps
            }
        }
    };
}

define_metrics!(
    qps:    tx-tx, rx-rx, err-err, cps-cps, kps-kps, conn-conn,noresponse-noresponse, key-key, nilconvert-nilconvert,uphit-uphit,hit-hit;
    num:    conn_num-conn;
    rtt:    avg-avg;
    ratio:  cache-hit
);
