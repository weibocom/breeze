use std::collections::LinkedList;
use std::time::{SystemTime, UNIX_EPOCH};
#[allow(dead_code)]
static RECONNECT_ERROR_CAP: usize = 5 as usize;
#[allow(dead_code)]
static RECONNECT_ERROR_WINDOW: u64 = 30 as u64;

#[allow(dead_code)]
enum BackendErrorType {
    ConnError = 0 as isize,
    //RequestError,
}

#[allow(dead_code)]
impl BackendErrorCounter {
    fn new(error_window_size: u64, error_type: BackendErrorType) -> BackendErrorCounter {
        BackendErrorCounter {
            error_window_size,
            error_count: 0 as usize,
            _error_type: error_type,
            error_time_list: LinkedList::new(),
            error_total_value: 0 as usize,
        }
    }

    fn add_error(&mut self, error_value: usize) {
        let current = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        self.judge_window();
        self.error_total_value += error_value;
        self.error_time_list.push_back((current, error_value));
        self.error_count += 1;
    }

    fn judge_error(&mut self, error_cap: usize) -> bool {
        self.judge_window();
        self.error_total_value >= error_cap
    }

    fn judge_window(&mut self) {
        let current = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        while !self.error_time_list.is_empty() {
            if self
                .error_time_list
                .front()
                .unwrap()
                .0
                .lt(&(current - self.error_window_size))
            {
                self.error_total_value -= self.error_time_list.front().unwrap().1;
                self.error_time_list.pop_front();
                self.error_count -= 1;
            } else {
                break;
            }
        }
    }
}
#[allow(dead_code)]
pub struct BackendErrorCounter {
    error_window_size: u64,
    error_count: usize,
    _error_type: BackendErrorType,
    error_time_list: LinkedList<(u64, usize)>,
    error_total_value: usize,
}
