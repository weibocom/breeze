use crossbeam_channel::unbounded;
use crossbeam_channel::{Receiver, Sender};
use once_cell::sync::OnceCell;
use std::net::UdpSocket;
use std::sync::Arc;
use std::thread;
use std::collections::HashMap;
use std::borrow::BorrowMut;
use std::cell::RefCell;
use std::time::{SystemTime, UNIX_EPOCH};

use thread_id;
use crate::metrics::{Metrics, MetricsConfig};

mod metrics;

static SENDER: OnceCell<MetricsSender> = OnceCell::new();
static METRICS_CONFIG: OnceCell<MetricsConfig> = OnceCell::new();
const METRICS_PREFIX: &'static str = &*"breeze.profile";


pub struct MetricsSender {
    sender: Arc<Sender<Metrics>>,
}

impl MetricsSender {
    thread_local! {
        // Could add pub to make it public to whatever Foo already is public to.
        static COUNT: RefCell<HashMap<String, usize>> = RefCell::new(HashMap::new());
        static LAST_SECOND: RefCell<u128> = RefCell::new(0 as u128);
    }
    fn new(config: &MetricsConfig) -> MetricsSender {
        let (new_sender, new_receiver) = unbounded::<Metrics>();
        let metrics_url = config.metrics_url.clone();
        let print_only = config.print_only;
        thread::spawn(move || {
            let mut metrics_collect_map = HashMap::<String, usize>::new();
            let mut metrics_stat_second_map = HashMap::<String, u128>::new();
            log::debug!("start send thread");
            let mut socket = None;
            loop {
                let received = new_receiver.recv();
                if received.is_err() {
                    continue;
                }
                let metrics = received.unwrap();
                if let Some(last_stat_second) = metrics_stat_second_map.get_mut(&*metrics.key) {
                    if metrics.stat_second.gt(last_stat_second) {
                        if !print_only {
                            if socket.as_ref().is_none() {
                                let udp_result = UdpSocket::bind(metrics_url.clone());
                                if udp_result.is_ok() {
                                    socket = Some(udp_result.unwrap());
                                } else {
                                    log::warn!("connect to metrics address {} error, {:?}", metrics_url.clone(), udp_result.unwrap_err());
                                }
                            }
                        }
                        if let Some(value) = metrics_collect_map.get_mut(&*metrics.key) {
                            let send_string = METRICS_PREFIX.clone().to_owned() + "." + &*metrics.key + ":" + &*value.to_string() + "|kv";
                            log::debug!("send string: {}", send_string);
                            if socket.as_ref().is_some() {
                                let result = socket.as_ref().unwrap().send(send_string.as_ref());
                                if result.is_err() {
                                    log::warn!("send metrics error: {:?}", result.unwrap_err());
                                    socket = None;
                                }
                            }
                            *value = metrics.value;
                        }
                        *last_stat_second = metrics.stat_second;
                    }
                    else {
                        if let Some(value) = metrics_collect_map.get_mut(&*metrics.key) {
                            *value += metrics.value;
                        }
                    }
                }
                else {
                    metrics_collect_map.insert(metrics.key.clone(), metrics.value);
                    metrics_stat_second_map.insert(metrics.key.clone(), metrics.stat_second);
                }
            }
        });
        MetricsSender {
            sender: Arc::new(new_sender),
        }
    }

    pub fn init(metrics_url: String) {
        METRICS_CONFIG.get_or_init(|| MetricsConfig::new(metrics_url));
    }

    pub fn sum(key: String, value: usize) {
        let config = METRICS_CONFIG.get().clone();
        if config.is_none() {
            return;
        }
        let sender = SENDER.get_or_init(|| MetricsSender::new(config.unwrap()));
        sender.sum_internal(key, value);
    }
    fn sum_internal(&self, key: String, value: usize) {
        let current_second = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros()/1000000;
        let mut collect_and_clear = false;
        MetricsSender::LAST_SECOND.with(|last_second| {
            let mut last_second = last_second.borrow_mut();
            if last_second.lt(&current_second) {
                *last_second = current_second;
                collect_and_clear = true;
            }
        });
        MetricsSender::COUNT.with(|count|{
            let mut count_map = count.borrow_mut();
            if collect_and_clear {
                for (key, value) in count_map.iter_mut() {
                    log::debug!("thread {} send: key = {}, value = {}", thread_id::get(), key.clone(), value);
                    let metrics = Metrics::new(key.clone(), *value, current_second);
                    let result = self.sender.send(metrics);
                    if result.is_err() {
                        log::warn!("collect message to metrics queue error");
                    }
                    *value = 0 as usize;
                }
            }
            if let Some(old_value) = count_map.get_mut(&*key) {
                *old_value += value;
            }
            else {
                count_map.insert(key, value);
            }
        });
    }
}

/*
#[cfg(test)]
mod tests {
    use crate::metrics::MetricsSender;
    use thread_id;
    use std::thread::JoinHandle;
    use std::time::Duration;

    #[test]
    fn test_sum() {
        let mut thread_vec: Vec<JoinHandle<()>> = Vec::new();
        for i in 1..5 {
            thread_vec.push(std::thread::spawn(move ||{
                for j in 1..10000000 {
                    //MetricsSender::sum(thread_id::get().to_string(), 1);
                    MetricsSender::sum("test".parse().unwrap(), 1);
                }
            }));
        }
        std::thread::sleep(Duration::from_secs(20));
    }
}
*/