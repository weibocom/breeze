use metrics::{Metric, Path};
use protocol::{Metric as ProtoMetric, MetricName, Operation, OPS};
macro_rules! define_metrics {
    ($($t:ident:$($name:ident-$key:expr),+);+) => {
        pub struct StreamMetrics {
            $(
                $(
                pub $name: Metric,
                )+
            )+
            ops: [Metric; OPS.len()],
            rtt: Metric,
        }
        impl StreamMetrics {
            // 使用所有的metrics之前，需要先check是否已注册。
            // 因为StremMetrics会依赖Metric.as_mut这个方法。
            pub fn check_registered(&mut self) -> bool {
                true $(
                    $(
                        && self.$name.check_registered()
                    )+
                )+ && self.rtt.check_registered() &&
                    self.ops.iter_mut().fold(true, |r, m| r && m.check_registered())
            }
            $(
            $(
            #[inline]
            pub fn $name(&self) -> &mut Metric {
                // Metric操作是原子计数的，因此unsafe不会导致UB。
               unsafe{self.$name.as_mut()}
            }
            )+
            )+
            #[inline]
            pub fn ops(&self, op:Operation) -> &mut Metric {
                // Metric操作是原子计数的，因此unsafe不会导致UB。
                debug_assert!(op.id() < self.ops.len());
                unsafe{self.ops.get_unchecked(op.id()).as_mut()}
            }
            // 所有命令所有业务的加权平均耗时
            #[inline]
            pub fn rtt(&self) -> &mut Metric {
                // Metric操作是原子计数的，因此unsafe不会导致UB。
                unsafe{self.rtt.as_mut()}
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
    qps:    tx-tx, rx-rx, err-err, cps-cps, kps-kps, conn-conn, key-key, nilconvert-nilconvert, inconsist-inconsist;
    num:    conn_num-conn, read-read, write-write, invalid_cmd-invalid_cmd, unsupport_cmd-unsupport_cmd;
    rtt:    avg-avg;
    ratio:  cache-hit;
    status: listen_failed-listen_failed
);

impl ProtoMetric<Metric> for StreamMetrics {
    #[inline]
    fn get(&self, name: MetricName) -> &mut Metric {
        match name {
            MetricName::Read => self.read(),
            MetricName::Write => self.write(),
            MetricName::NilConvert => self.nilconvert(),
            MetricName::Cache => self.cache(),
            MetricName::Inconsist => self.inconsist(),
        }
    }
    #[inline(always)]
    fn cache(&self, hit: bool) {
        *self.cache() += hit;
    }
    #[inline(always)]
    fn inconsist(&self, c: i64) {
        *self.inconsist() += c;
    }
}
