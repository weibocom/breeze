use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering::*};
use std::task::{ready, Context, Poll, Waker};

use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::time::{interval, Duration, Interval, MissedTickBehavior};

static REFRESH_CYCLE: AtomicUsize = AtomicUsize::new(0);
pub(crate) fn refresh_cycle() -> usize {
    REFRESH_CYCLE.load(Relaxed)
}

pub(crate) fn register(waker: Waker) -> u32 {
    let id = IDS.take();
    REGISTER
        .send(Operation::Register(id, waker))
        .expect("send failed");
    id
}

pub(crate) fn unregistering(id: u32) {
    REGISTER
        .send(Operation::Unregistering(id))
        .expect("send failed");
}
pub(crate) fn unregister(id: u32) {
    REGISTER
        .send(Operation::Unregister(id))
        .expect("send failed");
}

struct Ids {
    pool: spin::RwLock<Vec<u32>>,
    seq: AtomicU32,
}
impl Ids {
    fn take_from_pool(&self) -> Option<u32> {
        let pool = self.pool.try_read()?;
        if pool.len() == 0 {
            return None;
        }
        drop(pool);
        let mut pool = self.pool.try_write()?;
        pool.pop()
    }
    fn take(&self) -> u32 {
        let max = self.seq.load(Relaxed);
        if max < 1024 {
            return self.seq.fetch_add(1, AcqRel);
        }
        if let Some(id) = self.take_from_pool() {
            return id;
        }
        self.seq.fetch_add(1, AcqRel)
    }
    fn recall(&self, id: u32) -> bool {
        if let Some(mut pool) = self.pool.try_write() {
            pool.push(id);
            true
        } else {
            false
        }
    }
}
#[derive(Debug)]
enum Operation {
    Register(u32, Waker),
    Unregistering(u32),
    Unregister(u32),
}
static IDS: Ids = Ids {
    pool: spin::RwLock::new(Vec::new()),
    seq: AtomicU32::new(0),
};
#[ctor::ctor]
static REGISTER: UnboundedSender<Operation> = {
    let (tx, rx) = unbounded_channel();
    *RECEIVER.try_lock().expect("lock failed") = Some(rx);
    tx
};
use std::sync::Mutex;
static RECEIVER: Mutex<Option<UnboundedReceiver<Operation>>> = Mutex::new(None);

pub struct FixInterval {
    receiver: UnboundedReceiver<Operation>,
    cycle_complete: bool, //当前的cycle是否结束
    cycle: usize,
    idx: usize,
    idx_tick: usize,
    tick: Interval,
    // idx就为id
    registers: Vec<Option<Waker>>,
    unregistering: HashMap<u32, Waker>,
    available_ids: Vec<u32>,
}

const INTERVAL: Duration = Duration::from_millis(10);
pub(crate) const INTERVALS_PER_SEC: usize = 1000 / INTERVAL.as_millis() as usize;
const CYCLE: Duration = Duration::from_secs(30);
const TICKS_PER_CYCLE: usize = CYCLE.as_millis() as usize / INTERVAL.as_millis() as usize;
// 30秒钟一个周期：
// 1. 处理注册与注销请求
// 2. 唤醒所有注销中的waker（注销中的状态是closing，需要快速处理）
// 3. 唤醒部分已注册的waker
impl FixInterval {
    pub fn new() -> Self {
        let mut tick = interval(INTERVAL);
        tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let receiver = RECEIVER.try_lock().expect("lock").take().expect("init");
        Self {
            receiver,
            cycle: 0,
            cycle_complete: true,
            idx: 0,
            idx_tick: 0,
            tick,
            registers: Vec::new(),
            unregistering: Default::default(),
            available_ids: Default::default(),
        }
    }
    fn process(&mut self, op: Operation) {
        match op {
            Operation::Register(id, waker) => {
                self.registers.reserve(256);
                for _ in self.registers.len()..=id as usize {
                    self.registers.push(None);
                }
                self.registers[id as usize] = Some(waker);
            }
            Operation::Unregistering(id) => {
                let w = self.registers[id as usize].take().expect("waker");
                self.unregistering.insert(id, w);
            }
            Operation::Unregister(id) => {
                if self.unregistering.remove(&id).is_none() {
                    self.registers[id as usize].take().expect("waker");
                }
                if !IDS.recall(id) {
                    self.available_ids.push(id);
                }
                // pop掉末尾的empty
                while let Some(None) = self.registers.last() {
                    let none = self.registers.pop().expect("exists");
                    assert!(none.is_none());
                }
            }
        }
    }
    fn release_ids(&mut self) {
        while let Some(last) = self.available_ids.last() {
            if IDS.recall(*last) {
                self.available_ids.pop();
            }
        }
    }
    fn wake_unregistering(&mut self) {
        for waker in self.unregistering.values() {
            waker.wake_by_ref();
        }
    }
    fn guess_runs(&mut self) -> usize {
        let avg_run = (self.registers.len() as f64 / TICKS_PER_CYCLE as f64).max(1f64);
        (self.idx_tick as f64 * avg_run).ceil() as usize - self.idx
    }
}

impl Future for FixInterval {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        while let Poll::Ready(Some(op)) = self.receiver.poll_recv(cx) {
            self.process(op);
        }
        self.wake_unregistering();
        self.release_ids();

        self.idx_tick += 1;
        if self.cycle_complete {
            while self.idx_tick < TICKS_PER_CYCLE {
                ready!(self.tick.poll_tick(cx));
            }
            log::info!("refresh complete:{}", self);
            self.cycle = REFRESH_CYCLE.fetch_add(1, Relaxed) + 1;
            self.cycle_complete = false;
            self.idx_tick = 0;
            self.idx = 0;
        }

        let start = self.idx;
        let runs = self.guess_runs();
        let end = (self.idx + runs).min(self.registers.len());
        log::debug!("refresh tick:{} runs:{}", self, runs);
        self.idx = end;
        for waker in self.registers[start..end].iter() {
            if let Some(waker) = waker {
                waker.wake_by_ref();
            }
        }
        if self.idx == self.registers.len() {
            self.cycle_complete = true;
        }

        loop {
            ready!(self.tick.poll_tick(cx));
        }
    }
}

use std::fmt::{self, Display, Formatter};
impl Display for FixInterval {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "cycle-{}-{}/{} idx:{} registered:{} unregistering:{} avaliable_ids:{:?}, global cycle:{}",
            self.idx_tick,
            TICKS_PER_CYCLE,
            self.cycle,
            self.idx,
            self.registers.len(),
            self.unregistering.len(),
            self.available_ids,
            refresh_cycle(),
        )
    }
}
