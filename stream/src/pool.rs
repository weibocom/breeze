// #[derive(Debug, Clone)]
// pub enum LayerRole {
//     Master,
//     Slave,
//     MasterL1,
//     SlaveL1,
// }

// pub struct Pool<S> {
//     pub level: Level,
//     pub shards: Vec<S>,
// }

// impl<S> Pool<S>
// where
//     S: FakedClone,
// {
//     pub fn from(level: Level, shards: Vec<S>) -> Self {
//         Self { level, shards }
//     }

//     pub fn faked_clone(&self) -> Self {
//         Self {
//             level: self.level.clone(),
//             shards: self.shards.iter().map(|s| s.faked_clone()).collect(),
//         }
//     }
// }

// impl<S> Names for Pool<S>
// where
//     S: FakedClone + Addressed,
// {
//     fn names(&self) -> Vec<String> {
//         self.shards.iter().map(|s| s.addr().to_string()).collect()
//     }
// }

// impl<S> Addressed for Pool<S>
// where
//     S: FakedClone + Addressed,
// {
//     fn addr(&self) -> Address {
//         self.shards.addr()
//     }
// }
