use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct Namespace {
    #[serde(default)]
    pub(crate) basic: Basic,
    #[serde(default)]
    pub(crate) backends: Backends,

    // TODO 作为非主路径，实时构建更轻便？先和其他ns保持一致 fishermen
    #[serde(skip)]
    pub(crate) backends_flatten: Vec<String>,
    #[serde(skip)]
    pub(crate) backends_qsize: Vec<usize>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct Basic {
    /// msgque 可以读写的mq的名字
    #[serde(default)]
    pub(crate) keys: String,
    ///eg: mcq2,mcq3
    #[serde(default)]
    pub(crate) resource_type: String,
    #[serde(default)]
    pub(crate) timeout_read: u32,
    #[serde(default)]
    pub(crate) timeout_write: u32,
}

/// mcq 只支持512,1024,2048,4096,8192,16384,32768七个size
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct Backends {
    // 线上的que，可直接读写
    #[serde(default)]
    pub(crate) que_512: String,
    #[serde(default)]
    pub(crate) que_1024: String,
    #[serde(default)]
    pub(crate) que_2048: String,
    #[serde(default)]
    pub(crate) que_4096: String,
    #[serde(default)]
    pub(crate) que_8192: String,
    #[serde(default)]
    pub(crate) que_16384: String,
    #[serde(default)]
    pub(crate) que_32768: String,
}

impl Namespace {
    pub(crate) fn try_from(cfg: &str, _namespace: &str) -> Option<Self> {
        log::debug!("mq/{} parsing - cfg: {}", _namespace, cfg);
        match serde_yaml::from_str::<Namespace>(cfg) {
            Ok(mut ns) => {
                ns.flatten_backends();
                return Some(ns);
            }
            Err(_e) => {
                log::warn!("parse ns/{} failed: {:?}, cfg: {}", _namespace, _e, cfg);
                return None;
            }
        }
    }

    /// 获取所有的后端mq，按照size的顺序依次放置，但每个size中的mq会进行乱序
    #[inline]
    pub(crate) fn flatten_backends(&mut self) {
        self.backends_flatten = Vec::with_capacity(7);
        self.backends_qsize = Vec::with_capacity(7);
        // self.backends_qsize_flatten = Vec::with_capacity(8);
        // self.backends_qsize_index = Vec::with_capacity(8);

        self.collect_to_queue(512);
        self.collect_to_queue(1024);
        self.collect_to_queue(2048);
        self.collect_to_queue(4096);
        self.collect_to_queue(8192);
        self.collect_to_queue(16384);
        self.collect_to_queue(32768);
    }

    /// 将某个size的mq放入到整体的队列中，加入之前会进行乱序
    #[inline]
    fn collect_to_queue(&mut self, qsize: usize) {
        static EMPTY_QUE: String = String::new();
        let origin_que = match qsize {
            512 => &mut self.backends.que_512,
            1024 => &mut self.backends.que_1024,
            2048 => &mut self.backends.que_2048,
            4096 => &mut self.backends.que_4096,
            8192 => &mut self.backends.que_8192,
            16384 => &mut self.backends.que_16384,
            32768 => &mut self.backends.que_32768,
            _ => {
                debug_assert!(false, "not support qsize:{}", qsize);
                &EMPTY_QUE
            }
        };
        log::debug!("+++ size:{}, que:{:?}", qsize, origin_que);
        if origin_que.len() > 0 {
            // 调用顺序必须是按照size从小到大的顺序
            let last_qsize = self.backends_qsize.last().map(|s| s.clone()).unwrap_or(0);
            assert!(last_qsize < qsize, "{}/{}", last_qsize, qsize);

            self.backends_flatten.push(origin_que.to_string());
            self.backends_qsize.push(qsize);

            // 首先记录size的起始位置，然后将某个size的que中的ip随机排序，然后写入到整体的队列中
            // 最后将排序后的数据放入dest中
            // let qsize_pos = self.backends_qsize_flatten.len();
            // self.backends_qsize_index.push((qsize, qsize_pos));
            // let mut que: Vec<&str> = origin_que.split(",").collect();
            // que.shuffle(&mut rand::thread_rng());
            // let _ = que.iter().map(|q| {
            //     self.backends_flatten.push(q.clone());
            //     self.backends_qsize_flatten.push((q.clone(), qsize));
            // });
        }
    }
}
