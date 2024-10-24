use std::collections::HashMap;
use std::env;

use chrono::{Datelike, NaiveDate};
use discovery::dns;
use discovery::dns::IPPort;
use discovery::TopologyWrite;
use ds::MemGuard;
use protocol::kv::{ContextStatus, MysqlBuilder};
use protocol::vector::attachment::{BackendType, VAttach, VectorAttach};
use protocol::vector::redis::parse_vector_detail;
use protocol::vector::{command, CommandType, VectorCmd};
use protocol::Request;
use protocol::ResOption;
use protocol::Resource;
use protocol::{Operation, Protocol};
use sharding::hash::{Hash, HashKey};

use crate::dns::DnsConfig;
use crate::Timeout;
use crate::{Endpoint, Topology};
use protocol::vector::mysql::{SiSqlBuilder, SqlBuilder};

use super::config::VectorNamespace;
use super::strategy::Strategist;
use crate::kv::topo::Shards;
use crate::kv::KVCtx;
use crate::shards::Shard;
#[derive(Clone)]
pub struct VectorService<E, P> {
    shards: Shards<E>,
    si_shard: Vec<Shard<E>>,
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
            si_shard: Default::default(),
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
    fn has_attach(&self) -> bool {
        self.strategist.aggregation()
    }
}

impl<E, Req, P> VectorService<E, P>
where
    E: Endpoint<Item = Req>,
    Req: Request,
    P: Protocol,
{
    /// 从si表中计数来计算timeline待查询的参数
    fn get_timeline_query_param(
        &self,
        req: &Req,
        unbound_vcmd: &Option<VectorCmd>,
        round: u16,
    ) -> Result<(bool, u16, NaiveDate), protocol::Error> {
        use CommandType::*;
        // let vcmd = unbound_vcmd.as_ref().unwrap_or(&req.attach().vcmd);
        let vcmd = unbound_vcmd
            .as_ref()
            .or_else(|| Some(&req.attach().vcmd))
            .expect("vcmd");
        let cmd_props = command::get_cfg(req.op_code())?;

        // round为0时查timeline，肯定不是retrieve的aggregation请求；
        // 只有对retrieve类的aggregation请求，才会从si中获取timeline参数，其他的都不用；
        if round == 0 || !cmd_props.get_route().is_aggregation() {
            let date = self.strategist.get_date(vcmd.cmd, &vcmd.keys)?;
            let next_round = round == 0 && cmd_props.get_route().is_aggregation();
            return Ok((next_round, vcmd.limit(req.operation()) as u16, date));
        }

        assert!(req.operation().is_retrival(), "{vcmd}");
        assert!(vcmd.cmd == VRange || vcmd.cmd == VRangeTimeline, "{vcmd}");
        assert!(req.attachment().is_some(), "{vcmd}");

        //根据round从si获取timeline查询的参数
        let attach = req.attach().retrieve_attach();
        let si_items = attach.si();
        assert!(si_items.len() > 0, "si_items.len() = 0");
        assert!(
            round <= si_items.len() as u16,
            "round = {round}, si_items.len() = {}",
            si_items.len()
        );
        let si_item = &si_items[(round - 1) as usize];

        let year = si_item.date.year as u16 + 2000;
        //构建sql
        let limit = attach.left_count.min(si_item.count);
        assert!(si_item.count > 0, "{}", si_item.count);
        assert!(attach.left_count > 0, "{}", attach.left_count);

        let Some(date) = NaiveDate::from_ymd_opt(year.into(), si_item.date.month.into(), 1) else {
            return Err(protocol::Error::ResponseInvalidMagic);
        };
        Ok((round < si_items.len() as u16, limit, date))
    }

    fn get_shard(&self, req: &mut Req) -> Result<&Shard<E>, protocol::Error> {
        let (year, shard_idx) = if req.ctx_mut().runs == 0 {
            let vcmd = parse_vector_detail(****req, req.flag(), false)?;
            self.strategist.check_vector_cmd(&vcmd)?;
            //定位年库
            let date = self.strategist.get_date(vcmd.cmd, &vcmd.keys)?;

            let year = date.year() as u16;

            let shard_idx = self.shard_idx(req.hash());
            req.ctx_mut().year = year;
            req.ctx_mut().shard_idx = shard_idx as u16;

            let vector_builder = SqlBuilder::new(&vcmd, req.hash(), date, &self.strategist, 0)?;
            let cmd = MysqlBuilder::build_packets_for_vector(vector_builder)?;
            req.reshape(MemGuard::from_vec(cmd));

            (year, shard_idx)
        } else {
            (req.ctx_mut().year, req.ctx_mut().shard_idx as usize)
        };

        let shards = self.shards.get(year);
        let shard = shards.get(shard_idx).ok_or(protocol::Error::TopInvalid)?;
        log::debug!(
            "+++ mysql {} send {} year {} shards {:?} => {:?}",
            self.cfg.service,
            shard_idx,
            year,
            shards,
            req
        );
        Ok(shard)
    }

    /// <pre>
    /// 对于配置为aggregation的业务，指令的route可能有：timeline/main、si、aggregation(默认)三种；
    /// 1. 第一次请求
    ///     1.1. aggregation请求，对于下行vrange，首先请求si，然后请求timeline；对于vcard，直接请求si即结束；对于上行，需要先请求timeline，再请求si；
    ///     1.2. timeline请求，直接请求timeline表，下行失败重试一次，上行失败直接返回；
    ///     1.3. si请求，直接请求si表，下行失败重试一次，上行失败直接返回；
    /// 2. 第二次及以后请求
    ///     2.1. aggregation请求成功，对于下行vrange，继续请求timeline；对于上行，继续请求si；
    ///     2.2. aggregation请求失败，上行失败不会到这里；下行失败直接重试；
    ///     2.3. timeline、si请求成功，请求已结束，不会到这里；
    ///     2.3. timeline、si请求失败，下行请求重试，上行请求不会到这里；
    /// </pre>

    fn get_aggregation_shard(&self, req: &mut Req) -> Result<&Shard<E>, protocol::Error> {
        //分别代表请求的轮次和每轮重试次数
        let runs = req.ctx_mut().runs;
        //runs == 0 表示第一轮第一次请求
        let shard = if runs == 0 || (req.attachment().is_some() && req.attach().rsp_ok) {
            let shard = if runs == 0 {
                assert_eq!(*req.context_mut(), 0);
                assert_eq!(req.attachment(), None);

                let vcmd = parse_vector_detail(****req, req.flag(), true)?;
                self.strategist.check_vector_cmd(&vcmd)?;

                if vcmd.route.expect("aggregation").is_aggregation() {
                    // 目前非retrive请求，默认不开启聚合请求，避免sdk无操作
                    if !self.check_aggregation_request(req, true) {
                        log::info!("+++ found unsupported req:{}", req);
                        return Err(protocol::Error::ProtocolNotSupported);
                    }

                    req.set_next_round(true);
                    let operation = req.operation();
                    let attach = VectorAttach::new(operation, vcmd).to_attach();
                    let _ = req.attachment_mut().insert(attach);

                    self.reshape_and_get_shard(req, None, 0)?
                } else {
                    // 非aggregation请求的场景，不需要构建attachement
                    self.reshape_and_get_shard(req, Some(vcmd), 0)?
                }
            } else {
                assert!(req.get_next_round());
                assert!(req.attachment().is_some());
                self.reshape_and_get_shard(req, None, req.attach().round)?
            };

            //重新发送后，视作新的请求，重置响应和runs
            if req.attachment().is_some() {
                req.attach_mut().rsp_ok = false;
                req.attach_mut().round += 1;
            }
            req.ctx_mut().runs = 0;
            // req.set_fitst_try();
            shard
        } else {
            // 失败重试，对于kvector请求，只有下行请求才会重试
            assert!(req.operation().is_retrival(), "kvector req: {}", req);
            // 重试时，对于单库表访问，没有attachment
            let ctx = req.ctx();
            match ctx.backend_type.into() {
                BackendType::TimelineOrMain => {
                    // timeline重试
                    let shards = self.shards.get(ctx.year);
                    shards
                        .get(ctx.shard_idx as usize)
                        .ok_or(protocol::Error::TopInvalid)?
                }
                BackendType::Si => {
                    //si表重试
                    &self.si_shard[ctx.shard_idx as usize]
                }
                _ => {
                    panic!("malformed backend type");
                }
            }
        };

        log::debug!(
            "+++ mysql {} shards {:?} => {:?}",
            self.cfg.service,
            shard,
            req
        );
        Ok(shard)
    }

    fn reshape_and_get_shard(
        &self,
        req: &mut Req,
        unbound_vcmd: Option<VectorCmd>,
        round: u16,
    ) -> Result<&Shard<E>, protocol::Error> {
        // let vcmd = unbound_vcmd.as_ref().unwrap_or(&req.attach().vcmd);
        let vcmd = unbound_vcmd
            .as_ref()
            .or_else(|| Some(&req.attach().vcmd))
            .expect("vcmd");
        if self.will_access_timeline(&vcmd, req.operation(), round) {
            let (next_round, date) = self.reshap_request_timeline(req, unbound_vcmd, round)?;
            req.set_next_round(next_round);
            let shard = self.get_shard_timeline(req, &date)?;
            req.ctx_mut().backend_type = BackendType::TimelineOrMain;
            Ok(shard)
        } else {
            let next_round = self.reshape_request_si(req, unbound_vcmd)?;
            req.ctx_mut().backend_type = BackendType::Si;
            req.set_next_round(next_round);

            Ok(self.get_shard_si(req))
        }
    }

    /// 获取si shard，注意区分第一次获取和重试获取
    fn reshape_request_si(
        &self,
        req: &mut Req,
        unbound_vcmd: Option<VectorCmd>,
    ) -> Result<bool, protocol::Error> {
        let vcmd = unbound_vcmd
            .as_ref()
            .or_else(|| Some(&req.attach().vcmd))
            .expect("vcmd");
        assert!(vcmd.route.is_some(), "{vcmd}");
        let next_round = req.operation().is_retrival() && vcmd.route.expect("si").is_aggregation();

        let date = self.strategist.get_date(vcmd.cmd, &vcmd.keys)?;
        let si_sql = SiSqlBuilder::new(&vcmd, req.hash(), date, &self.strategist)?;
        let cmd = MysqlBuilder::build_packets_for_vector(si_sql)?;
        req.reshape(MemGuard::from_vec(cmd));

        Ok(next_round)
    }

    fn get_shard_si(&self, req: &mut Req) -> &Shard<E> {
        let si_shard_idx = self.strategist.si_distribution().index(req.hash());
        req.ctx_mut().shard_idx = si_shard_idx as u16;
        &self.si_shard[si_shard_idx]
    }

    /// 重新构建timeline请求，注意区分第一次获取和重试获取
    #[inline]
    fn reshap_request_timeline(
        &self,
        req: &mut Req,
        unbound_vcmd: Option<VectorCmd>,
        round: u16,
    ) -> Result<(bool, NaiveDate), protocol::Error> {
        let (next_round, limit, date) = self.get_timeline_query_param(req, &unbound_vcmd, round)?;

        let vcmd = unbound_vcmd
            .as_ref()
            .or_else(|| Some(&req.attach().vcmd))
            .expect("vcmd");
        let vector_builder =
            SqlBuilder::new(vcmd, req.hash(), date, &self.strategist, limit as u64)?;
        let cmd = MysqlBuilder::build_packets_for_vector(vector_builder)?;
        req.reshape(MemGuard::from_vec(cmd));
        req.ctx_mut().year = date.year() as u16;
        Ok((next_round, date))
    }

    #[inline]
    fn get_shard_timeline(
        &self,
        req: &mut Req,
        date: &NaiveDate,
    ) -> Result<&Shard<E>, protocol::Error> {
        let shard_idx = self.shard_idx(req.hash());
        req.ctx_mut().shard_idx = shard_idx as u16;
        let shards = self.shards.get(date.year() as u16);
        shards.get(shard_idx).ok_or(protocol::Error::TopInvalid)
    }

    #[inline]
    fn will_access_timeline(&self, vcmd: &VectorCmd, operation: Operation, round: u16) -> bool {
        vcmd.route
            .expect("aggregation")
            .current_backend_timeline(operation, round)
    }

    /// check是否支持该aggregation请求，目前默认只支持retrieve类聚合请求
    #[inline]
    fn check_aggregation_request(&self, req: &Req, aggregation_route: bool) -> bool {
        if req.operation().is_retrival() {
            // retrival 请求总是支持
            true
        } else if aggregation_route && self.strategist.aggregation() {
            // 对非retrieve类的聚合请求，在aggregation策略的topo中，设置环境变量aggregation_store_enable为true，才支持访问；
            // 目前线上业务都不打开，故线上应该没有此类访问；待有需求再打开
            log::info!(
                "AGGENGATION_STORE: {}",
                env::var("AGGENGATION_STORE").unwrap_or("not-set".to_string())
            );
            env::var("AGGENGATION_STORE")
                .unwrap_or("false".to_string())
                .parse::<bool>()
                .unwrap_or(false)
        } else {
            true
        }
    }
}

impl<E, Req, P> Endpoint for VectorService<E, P>
where
    E: Endpoint<Item = Req>,
    Req: Request,
    P: Protocol,
{
    type Item = Req;

    /// <pre>
    /// 发送请求有如下之中类型：
    ///   1. 配置为aggregation
    ///     1.1 请求aggregation（默认），对于下行请求，先请求si，再请求timeline；对于上行请求，先请求timeline，成功再请求si（低优先级，线上无聚合上行，后续支持）；
    ///     1.2 请求timeline，直接请求timeline库表，上行失败直接返回，下行失败重试一次；
    ///     1.3 请求si，直接请求si库表，上行失败直接返回，下行失败重试一次；
    ///   2. 配置为非aggregation，当前只有vectortime
    ///     2.1 没有请求route，上行失败直接返回，下行失败重试一次。
    /// </pre>
    fn send(&self, mut req: Self::Item) {
        log::info!("+++ will send req: {}", req);

        let shard = if !self.strategist.aggregation() {
            self.get_shard(&mut req)
        } else {
            self.get_aggregation_shard(&mut req)
        };
        let shard = match shard {
            Ok(shard) => shard,
            Err(e) => {
                req.ctx_mut().error = match e {
                    protocol::Error::TopInvalid => ContextStatus::TopInvalid,
                    _ => ContextStatus::ReqInvalid,
                };
                req.try_next(false);
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
            log::info!("+++ send to {}, req: {}", endpoint.addr(), req);
            endpoint.send(req)
        } else {
            let ctx = req.ctx_mut();
            ctx.runs += 1;
            log::info!("+++ send to master {}, req: {}", shard.master.addr(), req);
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
        (self.shards.len() + self.si_shard.len()) != self.cfg.shards_url.len()
            || self.cfg.need_load()
    }
    fn load(&mut self) -> bool {
        self.cfg.load_guard().check_load(|| self.load_inner())
    }
    fn update(&mut self, namespace: &str, cfg: &str) {
        if let Some(ns) = VectorNamespace::try_from(cfg) {
            let Some(strategist) = Strategist::try_from(&ns) else {
                return;
            };
            self.strategist = strategist;
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
        if !self.load_inner_si() {
            return false;
        }
        assert_eq!(
            self.shards.len() + self.si_shard.len(),
            self.cfg.shards_url.len()
        );

        log::info!("{} load complete. dropping:{:?}", self.cfg.service, {
            old.retain(|_k, v| v.len() > 0);
            old.keys()
        });
        true
    }

    #[inline]
    fn load_inner_si(&mut self) -> bool {
        // 所有的ip要都能解析出主从域名
        let mut addrs = Vec::with_capacity(self.cfg.si_backends.len());
        for shard in &self.cfg.si_backends {
            let shard: Vec<&str> = shard.split(",").collect();
            if shard.len() < 2 {
                log::warn!("{} si both master and slave required.", self.cfg.service);
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
            addrs.push((master, slaves));
        }

        // 到这之后，所有的shard都能解析出ip
        let mut old = HashMap::with_capacity(addrs.len());
        self.si_shard.split_off(0).into_iter().for_each(|shard| {
            old.entry(shard.master.addr().to_string())
                .or_insert(Vec::new())
                .push(shard.master);
            for endpoint in shard.slaves.into_inner() {
                let addr = endpoint.addr().to_string();
                // 一个ip可能存在于多个域名中。
                old.entry(addr).or_insert(Vec::new()).push(endpoint);
            }
        });

        // 遍历所有的shards_url
        for (master_addr, slaves) in addrs {
            assert_ne!(master_addr.len(), 0);
            assert_ne!(slaves.len(), 0);
            // 用户名和密码
            let res_option = ResOption {
                token: self.cfg.basic.si_password.clone(),
                username: self.cfg.basic.si_user.clone(),
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
                self.cfg.basic.region_enabled,
            );
            self.si_shard.push(shard);
        }

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
            && (self.shards.len() + self.si_shard.len()) == self.cfg.shards_url.len()
            && self.shards.inited()
    }
}
