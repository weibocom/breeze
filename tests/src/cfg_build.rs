use std::{
    fs::File,
    io::{BufWriter, Write},
};

#[ignore]
#[test]
fn build_redis_cfg() {
    let start_port = 58064;
    let end_port = 58319 + 1;
    let hash = "fnv1a_64";
    let dist = "ketama_origin";
    let namespace = "mapi";

    let mut ports = String::with_capacity(1024);
    for p in start_port..end_port {
        ports += &format!("{},", p);
    }
    let _ = ports.split_off(ports.len() - 1);

    let mut shards = String::with_capacity(4096);
    for p in start_port..end_port {
        shards += &format!(
            "  - m{}.test:{},s{}.:{} node{}\n",
            p,
            p,
            p,
            p,
            (p - start_port + 1)
        );
    }

    let mut cfg_str = String::with_capacity(8192);
    cfg_str += "basic:\n";
    cfg_str += "  access_mod: rw\n";
    cfg_str += &format!("  hash: {}\n", hash);
    cfg_str += &format!("  distribution: {}\n", dist);
    cfg_str += &format!("  listen: {}\n", ports);
    cfg_str += "  resource_type: eredis\n";
    cfg_str += "  timeout_ms_master: 500\n  timeout_ms_slave: 500\n";

    cfg_str += "backends:\n";
    cfg_str += &shards;

    let cfg_file = format!("../static.{}.cfg", namespace);
    let file = File::create(cfg_file).unwrap();
    let mut writer = BufWriter::new(file);
    writer.write_all(cfg_str.as_bytes()).unwrap();
    writer.flush().unwrap();

    let mut shards_4_table = String::with_capacity(8192);
    for p in start_port..end_port {
        shards_4_table += &format!(
            "m{}.test:{},s{}.test:{} node{}\n",
            p,
            p,
            p,
            p,
            (p - start_port + 1)
        );
    }
    let cfg_file = format!("../static.{}.cfg.table", namespace);
    let file = File::create(cfg_file).unwrap();
    let mut writer = BufWriter::new(file);
    writer
        .write_all(format!("shards:\n{}", shards_4_table).as_bytes())
        .unwrap();
    writer.flush().unwrap();
}
