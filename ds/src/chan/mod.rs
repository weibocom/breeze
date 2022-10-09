pub type Sender<T> = tokio::sync::mpsc::Sender<T>;
pub type Receiver<T> = tokio::sync::mpsc::Receiver<T>;
pub use tokio::sync::mpsc::channel as bounded;

pub mod mpsc;
