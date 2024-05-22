use std::collections::HashMap;

use chrono::Datelike;
use discovery::dns;
use discovery::dns::IPPort;
use discovery::TopologyWrite;
use ds::MemGuard;
use protocol::kv::{ContextStatus, MysqlBuilder};
use protocol::vector::redis::{build_attachment, parse_vector_detail};
use protocol::Protocol;
use protocol::Request;
use protocol::ResOption;
use protocol::Resource;
use sharding::hash::{Hash, HashKey};

use crate::dns::DnsConfig;
use crate::Timeout;
use crate::{Endpoint, Topology};
use protocol::vector::mysql::SqlBuilder;

use super::config::VectorNamespace;
use super::strategy::Strategist;
use crate::kv::topo::Shards;
use crate::kv::KVCtx;
use crate::shards::Shard;
#[derive(Clone)]
pub struct VectorService<E, P> {
    shards: Shards<E>,
    strategist: Strategist,
    parser: P,
    cfg: Box<DnsConfig<VectorNamespace>>,
}

impl<E, P> From<P> for VectorService<E, P> {
    #[inline]
    fn from(parser: P) -> Self {
        Self {
            parser,
            shards: Default::default(),
            strategist: Default::default(),
            cfg: Default::default(),
        }
    }
}

impl<E, P> Hash for VectorService<E, P>
where
    E: Endpoint,
    P: Protocol,
{
    #[inline]
    fn hash<K: HashKey>(&self, k: &K) -> i64 {
        self.strategist.hasher().hash(k)
    }
}

impl<E, Req, P> Topology for VectorService<E, P>
where
    E: Endpoint<Item = Req>,
    Req: Request,
    P: Protocol,
{
}

impl<E, Req, P> Endpoint for VectorService<E, P>
where
    E: Endpoint<Item = Req>,
    Req: Request,
    P: Protocol,
{
    type Item = Req;

    fn send(&self, mut req: Self::Item) {
        let shard = (|| -> Result<&Shard<E>, protocol::Error> {
            //需要请求多轮的场景处理逻辑看作是一次新的请求，除外出错重试
            let more = self.strategist.more() && req.retry_rsp_ok();
            let (year, shard_idx) = if req.ctx_mut().runs == 0 || more {
                let (vcmd, date, shard_idx) = if more {
                    //非第一轮请求
                    let vcmd = parse_vector_detail(**req.origin_data(), req.flag())?;

                    let ctx = req.ctx_mut();
                    let date = self.strategist.get_next_date(ctx.year, ctx.month);
                    ctx.runs = 0;
                    (vcmd, date, ctx.shard_idx)
                } else {
                    let vcmd = parse_vector_detail(***req, req.flag())?;
                    if self.strategist.more() {
                        // 批量获取场景必须提供limit
                        let Some(limit) = vcmd.limit() else {
                            return Err(protocol::Error::RequestProtocolInvalid);
                        };
                        //需要在buildsql之前设置
                        *req.extra_ctx_mut() = limit as u64;
                    }
                    //定位年库
                    let date = self.strategist.get_date(&vcmd.keys)?;
                    let shard_idx = self.shard_idx(req.hash());

                    req.ctx_mut().shard_idx = shard_idx as u16;
                    (vcmd, date, shard_idx as u16)
                };

                // 首次访问，如果协议、策略层面需要，设置attachment，后续在解析响应后更新
                if self.strategist.more()
                    && req.operation().is_retrival()
                    && req.attachment().is_none()
                {
                    req.attach(build_attachment(&vcmd).to_vec());
                }

                req.ctx_mut().year = date.year() as u16;
                req.ctx_mut().month = date.month() as u8;

                let vector_builder =
                    SqlBuilder::new(&vcmd, req.hash(), date, &self.strategist, *req.extra_ctx())?;
                let cmd = MysqlBuilder::build_packets_for_vector(vector_builder)?;
                req.reshape(MemGuard::from_vec(cmd));

                (date.year() as u16, shard_idx)
            } else {
                (req.ctx_mut().year, req.ctx_mut().shard_idx)
            };

            let shards = self.shards.get(year);
            if shards.len() == 0 {
                return Err(protocol::Error::TopInvalid);
            }
            let shard_idx = shard_idx as usize;
            debug_assert!(
                shard_idx < shards.len(),
                "mysql: {}/{} req:{:?}",
                shard_idx,
                shards.len(),
                req
            );
            let shard = unsafe { shards.get_unchecked(shard_idx) };
            log::debug!(
                "+++ mysql {} send {} year {} shards {:?} => {:?}",
                self.cfg.service,
                shard_idx,
                year,
                shards,
                req
            );
            Ok(shard)
        })();
        let shard = match shard {
            Ok(shard) => shard,
            Err(e) => {
                req.ctx_mut().error = match e {
                    protocol::Error::TopInvalid => ContextStatus::TopInvalid,
                    _ => ContextStatus::ReqInvalid,
                };
                req.on_err(e);
                return;
            }
        };
        if shard.has_slave() && !req.operation().is_store() {
            if *req.context_mut() == 0 {
                if let Some(quota) = shard.slaves.quota() {
                    req.quota(quota);
                }
            }
            let ctx = req.ctx_mut();
            let (idx, endpoint) = if ctx.runs == 0 {
                shard.select()
            } else {
                if (ctx.runs as usize) < shard.slaves.len() {
                    shard.next(ctx.idx as usize, ctx.runs as usize)
                } else {
                    // 说明只有一个从，并且从访问失败了，会通过主访问。
                    (ctx.idx as usize, &shard.master)
                }
            };
            ctx.idx = idx as u8;
            ctx.runs += 1;
            // 只重试一次，重试次数过多，可能会导致雪崩。如果不重试，现在的配额策略在当前副本也只会连续发送四次请求，问题也不大
            let try_next = ctx.runs == 1;
            req.try_next(try_next);
            endpoint.send(req)
        } else {
            shard.master().send(req);
        }
    }

    fn shard_idx(&self, hash: i64) -> usize {
        self.strategist.distribution().index(hash)
    }
}

impl<E, P> TopologyWrite for VectorService<E, P>
where
    P: Protocol,
    E: Endpoint,
{
    fn need_load(&self) -> bool {
        self.shards.len() != self.cfg.shards_url.len() || self.cfg.need_load()
    }
    fn load(&mut self) -> bool {
        self.cfg.load_guard().check_load(|| self.load_inner())
    }
    fn update(&mut self, namespace: &str, cfg: &str) {
        if let Some(ns) = VectorNamespace::try_from(cfg) {
            self.strategist = Strategist::try_from(&ns);
            self.cfg.update(namespace, ns);
        }
    }
}
impl<E, P> VectorService<E, P>
where
    P: Protocol,
    E: Endpoint,
{
    // #[inline]
    fn take_or_build(
        &self,
        old: &mut HashMap<String, Vec<E>>,
        addr: &str,
        timeout: Timeout,
        res: ResOption,
    ) -> E {
        match old.get_mut(addr).map(|endpoints| endpoints.pop()) {
            Some(Some(end)) => end,
            _ => E::build_o(
                &addr,
                self.parser.clone(),
                Resource::Mysql,
                &self.cfg.service,
                timeout,
                res,
            ),
        }
    }
    #[inline]
    fn load_inner(&mut self) -> bool {
        // 所有的ip要都能解析出主从域名
        let mut addrs = Vec::with_capacity(self.cfg.backends.len());
        for (interval, shard) in &self.cfg.backends {
            let mut addrs_per_interval = Vec::with_capacity(shard.len());
            for shard in shard.iter() {
                let shard: Vec<&str> = shard.split(",").collect();
                if shard.len() < 2 {
                    log::warn!("{} both master and slave required.", self.cfg.service);
                    return false;
                }
                let master_url = &shard[0];
                let mut master = String::new();
                dns::lookup_ips(master_url.host(), |ips| {
                    if ips.len() > 0 {
                        master = ips[0].to_string() + ":" + master_url.port();
                    }
                });
                let mut slaves = Vec::with_capacity(8);
                for url_port in &shard[1..] {
                    let url = url_port.host();
                    let port = url_port.port();
                    use ds::vec::Add;
                    dns::lookup_ips(url, |ips| {
                        for ip in ips {
                            slaves.add(ip.to_string() + ":" + port);
                        }
                    });
                }
                if master.len() == 0 || slaves.len() == 0 {
                    log::warn!(
                        "master:({}=>{}) or slave ({:?}=>{:?}) not looked up",
                        master_url,
                        master,
                        &shard[1..],
                        slaves
                    );
                    return false;
                }
                addrs_per_interval.push((master, slaves));
            }
            addrs.push((interval, addrs_per_interval));
        }

        // 到这之后，所有的shard都能解析出ip
        let mut old = HashMap::with_capacity(self.shards.len());
        for shard in self.shards.take() {
            old.entry(shard.master.addr().to_string())
                .or_insert(Vec::new())
                .push(shard.master);
            for endpoint in shard.slaves.into_inner() {
                let addr = endpoint.addr().to_string();
                // 一个ip可能存在于多个域名中。
                old.entry(addr).or_insert(Vec::new()).push(endpoint);
            }
        }

        for (interval, addrs_per_interval) in addrs {
            let mut shards_per_interval = Vec::with_capacity(addrs_per_interval.len());
            // 遍历所有的shards_url
            for (master_addr, slaves) in addrs_per_interval {
                assert_ne!(master_addr.len(), 0);
                assert_ne!(slaves.len(), 0);
                // 用户名和密码
                let res_option = ResOption {
                    token: self.cfg.basic.password.clone(),
                    username: self.cfg.basic.user.clone(),
                };
                let master = self.take_or_build(
                    &mut old,
                    &master_addr,
                    self.cfg.timeout_master(),
                    res_option.clone(),
                );
                // slave
                let mut replicas = Vec::with_capacity(8);
                for addr in slaves {
                    let slave = self.take_or_build(
                        &mut old,
                        &addr,
                        self.cfg.timeout_slave(),
                        res_option.clone(),
                    );
                    replicas.push(slave);
                }

                use crate::PerformanceTuning;
                let shard = Shard::selector(
                    self.cfg.basic.selector.tuning_mode(),
                    master,
                    replicas,
                    false,
                );
                shards_per_interval.push(shard);
            }
            self.shards.push((interval, shards_per_interval));
        }
        assert_eq!(self.shards.len(), self.cfg.shards_url.len());
        log::info!("{} load complete. dropping:{:?}", self.cfg.service, {
            old.retain(|_k, v| v.len() > 0);
            old.keys()
        });
        true
    }
}
impl<E, P> discovery::Inited for VectorService<E, P>
where
    E: discovery::Inited,
{
    // 每一个域名都有对应的endpoint，并且都初始化完成。
    #[inline]
    fn inited(&self) -> bool {
        // 每一个分片都有初始, 并且至少有一主一从。
        self.shards.len() > 0
            && self.shards.len() == self.cfg.shards_url.len()
            && self.shards.inited()
    }
}
