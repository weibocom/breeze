// use std::{u64};
// use serde::{Serialize, Deserialize};



// #[derive(Serialize, Deserialize, Debug)]
// pub struct MemcacheConf {
//     hash: String,  // eg: bkdr
//     distribution: String,  //eg: ketama
//     hash_tag: String,      //eg: user
//     timeout: i32, // unit: mills
//     exptime: i64, 
//     master: Vec<String>,
//     #[serde(default)]
//     master_l1: Vec<Vec<String>>,
//     #[serde(default)]
//     slave: Vec<String>,
// }