use crossbeam_channel::unbounded;
use crossbeam_channel::{Receiver, Sender};
use once_cell::sync::OnceCell;
use std::net::UdpSocket;
use std::sync::Arc;
use std::thread;

static SENDER: OnceCell<MetricsSender> = OnceCell::new();

struct Metrics {
    key: String,
    value: String,
}

impl Metrics {
    fn new(key: String, value: String) -> Metrics {
        Metrics { key, value }
    }
}

pub(crate) struct MetricsSender {
    sender: Arc<Sender<Metrics>>,
    init: bool,
}

impl MetricsSender {
    fn init() -> MetricsSender {
        let (new_sender, new_receiver) = unbounded::<Metrics>();
        thread::spawn(move || {
            println!("start send thread");
            let mut socket = UdpSocket::bind("test_metrics:1234").unwrap();
            //let mut socket = UdpSocket::bind("127.0.0.1:1234").unwrap();
            loop {
                let received = new_receiver.recv();
                if received.is_err() {
                    continue;
                }
                let metrics = received.unwrap();
                let send_string = metrics.key + ":" + &*metrics.value + "|kv";
                println!("send string: {}", send_string);
                socket.send(send_string.as_ref());
            }
        });
        MetricsSender {
            sender: Arc::new(new_sender),
            init: true,
        }
    }

    pub fn send(key: String, value: String) {
        let sender = SENDER.get_or_init(|| MetricsSender::init());
        sender.send_internal(key, value);
    }
    fn send_internal(&self, key: String, value: String) {
        let metrics = Metrics::new(key, value);
        self.sender.send(metrics);
    }
}
