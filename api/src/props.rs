use std::env;

pub fn set_evn(key: String, val: String) {
    if env::var(&key).is_err() {
        env::set_var(&key, val);
    }
}

pub fn from_evn(key: String, default_val: String) -> String {
    env::var(key).unwrap_or(default_val)
}
