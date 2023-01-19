use metrics::{Metric, Path};
use protocol::{Metric as ProtoMetric, MetricName, Operation, OPS};
macro_rules! define_metrics {
    ($($t:ident:$($name:ident-$key:expr),+);+) => {
        pub struct StreamMetrics {
            $(
                $(
                $name: Metric,
                )+
            )+
            ops: [Metric; OPS.len()],
            rtt: Metric,
        }
        impl StreamMetrics {
            $(
            $(
            #[inline]
            pub fn $name(&self) -> &mut Metric {
                // Metric操作是原子计数的，因此unsafe不会导致UB。
               self.$name.as_mut()
            }
            )+
            )+
            #[inline]
            pub fn ops(&self, op:Operation) -> &mut Metric {
                // Metric操作是原子计数的，因此unsafe不会导致UB。
                assert!(op.id() < self.ops.len());
                unsafe{self.ops.get_unchecked(op.id()).as_mut()}
            }
            // 所有命令所有业务的加权平均耗时
            #[inline]
            pub fn rtt(&self) -> &mut Metric {
                // Metric操作是原子计数的，因此unsafe不会导致UB。
                self.rtt.as_mut()
            }
            pub fn new(path:&Path) -> Self {
                let ops: [Metric; OPS.len()] =
                    array_init::array_init(|idx| path.rtt(OPS[idx].name()));
                Self {
                    ops,
                    rtt: path.pop().rtt("cmd_all"),
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
    qps:    tx-tx, rx-rx, err-err, cps-cps, kps-kps, conn-conn, key-key, nilconvert-nilconvert;
    num:    conn_num-conn, read-read, write-write, invalid_cmd-invalid_cmd;
    rtt:    avg-avg;
    ratio:  cache-hit
);

impl ProtoMetric<Metric> for StreamMetrics {
    #[inline]
    fn get(&self, name: MetricName) -> &mut Metric {
        match name {
            MetricName::Read => self.read(),
            MetricName::Write => self.write(),
            MetricName::NilConvert => self.nilconvert(),
            MetricName::Cache => self.cache(),
        }
    }
}
