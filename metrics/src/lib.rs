use crossbeam_channel::unbounded;
use crossbeam_channel::{Receiver, Sender};
use once_cell::sync::OnceCell;
use std::borrow::BorrowMut;
use std::cell::RefCell;
use std::collections::HashMap;
use std::net::UdpSocket;
use std::sync::Arc;
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::metrics::CalculateMethod::Avg;
use crate::metrics::{CalculateMethod, Metrics, MetricsConfig};
use thread_id;

mod metrics;

static SENDER: OnceCell<MetricsSender> = OnceCell::new();
static METRICS_CONFIG: OnceCell<MetricsConfig> = OnceCell::new();
const METRICS_PREFIX: &'static str = &*"breeze.profile.test.nvm";

pub struct MetricsSender {
    sender: Arc<Sender<Metrics>>,
}

impl MetricsSender {
    thread_local! {
        // Could add pub to make it public to whatever Foo already is public to.
        static CALCULATE_RESULT: RefCell<HashMap<String, (f64, usize, CalculateMethod)>> = RefCell::new(HashMap::new());
        static LAST_SECOND: RefCell<u128> = RefCell::new(0 as u128);
    }
    fn new(config: &MetricsConfig) -> MetricsSender {
        let (new_sender, new_receiver) = unbounded::<Metrics>();
        let metrics_url = config.metrics_url.clone();
        let print_only = config.print_only;
        let local_address = local_ip_address::local_ip().unwrap().to_string();
        let replaced_address = local_address.replace(".", "_");
        let full_prefix = METRICS_PREFIX.to_owned() + ".byhost." + &*replaced_address + ".";
        log::debug!(
            "local ip address : {}, full metrics prefix: {}",
            local_address.clone(),
            full_prefix.clone()
        );
        thread::spawn(move || {
            let mut metrics_collect_map = HashMap::<String, (f64, usize, CalculateMethod)>::new();
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
                                let udp_result = UdpSocket::bind(local_address.clone() + ":34254");
                                if udp_result.is_ok() {
                                    let mut udp_socket = udp_result.unwrap();
                                    udp_socket
                                        .connect(metrics_url.clone())
                                        .expect("connect to metrics error");
                                    socket = Some(udp_socket);
                                } else {
                                    log::warn!("bind error, {:?}", udp_result.unwrap_err());
                                }
                            }
                        }
                        if let Some(value) = metrics_collect_map.get_mut(&*metrics.key) {
                            let send_string = full_prefix.clone()
                                + &*metrics.key
                                + ":"
                                + &*(f64::trunc(value.0 * 100.0 as f64) / 100.0 as f64).to_string()
                                + "|kv\n";
                            //log::debug!("send string: {}", send_string);
                            if socket.as_ref().is_some() {
                                let result = socket.as_ref().unwrap().send(send_string.as_ref());
                                if result.is_err() {
                                    log::warn!("send metrics error: {:?}", result.unwrap_err());
                                    socket = None;
                                }
                            }
                            value.0 = metrics.value;
                            value.1 = metrics.count;
                        }
                        *last_stat_second = metrics.stat_second;
                    } else {
                        if let Some(value) = metrics_collect_map.get_mut(&*metrics.key) {
                            match value.2 {
                                CalculateMethod::Sum => {
                                    value.0 += metrics.value;
                                    value.1 += metrics.count;
                                }
                                CalculateMethod::Avg => {
                                    value.0 = ((value.0 * value.1 as f64)
                                        + (metrics.value * metrics.count as f64))
                                        / (value.1 as f64 + metrics.count as f64);
                                    value.1 += metrics.count;
                                }
                            }
                        }
                    }
                } else {
                    metrics_collect_map.insert(
                        metrics.key.clone(),
                        (metrics.value, metrics.count, metrics.method),
                    );
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
        sender.calculate(key, value, CalculateMethod::Sum);
    }

    pub fn avg(key: String, value: usize) {
        let config = METRICS_CONFIG.get().clone();
        if config.is_none() {
            return;
        }
        let sender = SENDER.get_or_init(|| MetricsSender::new(config.unwrap()));
        sender.calculate(key, value, Avg);
    }

    fn calculate(&self, key: String, value: usize, method: CalculateMethod) {
        let current_second = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros()
            / 1000000;
        let mut collect_and_clear = false;
        MetricsSender::LAST_SECOND.with(|last_second| {
            let mut last_second = last_second.borrow_mut();
            if last_second.lt(&current_second) {
                *last_second = current_second;
                collect_and_clear = true;
            }
        });
        MetricsSender::CALCULATE_RESULT.with(|calculate_result| {
            let mut calculate_map = calculate_result.borrow_mut();
            if collect_and_clear {
                for (key, value) in calculate_map.iter_mut() {
                    //log::debug!(
                    //    "thread {} send: key = {}, value = {}, count = {}, method = {}",
                    //    thread_id::get(),
                    //    key.clone(),
                    //    value.0,
                    //    value.1,
                    //    value.2
                    //);
                    let metrics =
                        Metrics::new(key.clone(), value.0, value.1, value.2, current_second);
                    let result = self.sender.send(metrics);
                    if result.is_err() {
                        log::warn!("collect message to metrics queue error");
                    }
                    value.0 = 0 as f64;
                    value.1 = 0 as usize;
                }
            }
            if let Some(old_value) = calculate_map.get_mut(&*key) {
                match old_value.2 {
                    CalculateMethod::Sum => {
                        old_value.0 += value as f64;
                    }
                    CalculateMethod::Avg => {
                        old_value.0 = ((old_value.0 * old_value.1 as f64) + value as f64)
                            / (old_value.1 as f64 + 1.0);
                    }
                }
                old_value.1 += 1;
            } else {
                calculate_map.insert(key.clone(), (value as f64, 1 as usize, method));
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use crate::MetricsSender;
    use std::thread::JoinHandle;
    use std::time::Duration;
    use thread_id;

    #[test]
    fn test_sum() {
        let mut thread_vec: Vec<JoinHandle<()>> = Vec::new();
        MetricsSender::init("logtailer.monitor.weibo.com:8333".parse().unwrap());
        for i in 1..5 {
            thread_vec.push(std::thread::spawn(move || {
                for j in 1..10000000 {
                    //MetricsSender::sum(thread_id::get().to_string(), 1);
                    MetricsSender::avg("test".parse().unwrap(), 3);
                }
            }));
        }
        std::thread::sleep(Duration::from_secs(20));
    }
}
