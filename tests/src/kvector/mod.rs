use std::{fs::File, io::BufRead};

use chrono::TimeZone;
use endpoint::kv::uuid::Uuid;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]

struct LikeByMe {
    #[serde(default)]
    id: usize,
}

const MESH_LOG_PREFIX_LEN: usize =
    "2024-11-08 16:01:23 [INFO] consistencyNewLikeByMeVectorList listMesh:".len();
const TCP_LOG_PREFIX_LENT: usize =
    "2024-11-08 16:01:23 [INFO] consistencyNewLikeByMeVectorList listTcp:".len();

#[test]
fn parse_like_by_me() {
    let file_name = "tmp_data/vector.log";
    let file = File::open(file_name).unwrap();
    let reader = std::io::BufReader::new(file);

    let mut mesh_ids = Vec::new();
    let mut _tcp_ids = Vec::new();
    let mut count = 0;
    const MAX_COUNT: usize = 1;

    for line in reader.lines() {
        let line = line.unwrap();
        let (prefix_len, is_mesh_list) = match line.contains("listMesh") {
            true => (MESH_LOG_PREFIX_LEN, true),
            false => (TCP_LOG_PREFIX_LENT, false),
        };
        let likes_json = &line[prefix_len..];
        let suffix = likes_json.rfind("]").unwrap() + 1;
        let likes_json = &likes_json[..suffix];
        // println!("line: {}", line);

        // 顺序为先一行mesh, 后一行tcp
        let ids = parse_line(likes_json);
        if is_mesh_list {
            mesh_ids = ids;
            // println!("mesh_ids: {:?}", mesh_ids);
            continue;
        } else {
            _tcp_ids = ids;
            // println!("tcp_ids: {:?}", tcp_ids);

            let diff_in_mesh: Vec<usize> = mesh_ids
                .clone()
                .into_iter()
                .filter(|id| !_tcp_ids.contains(id))
                .collect();
            let diff_in_tcp: Vec<usize> = _tcp_ids
                .clone()
                .into_iter()
                .filter(|id| !mesh_ids.contains(id))
                .collect();

            if diff_in_mesh.len() > 0 || diff_in_tcp.len() > 0 {
                println!("mesh/tcp count:{}/{}", mesh_ids.len(), _tcp_ids.len());
                println!("mesh more ids:{:?}", diff_in_mesh);
                println!("tcp more ids: {:?}", diff_in_tcp);
                println!("mesh:{:?}", mesh_ids);
                println!(" tcp:{:?}", _tcp_ids);
                count += 1;
                if count >= MAX_COUNT {
                    break;
                }

                mesh_ids.clear();
                _tcp_ids.clear();
            }
            continue;
        }
    }
}

fn parse_line(line: &str) -> Vec<usize> {
    let mut ids = Vec::new();
    let like_by_mes: Vec<LikeByMe> = serde_json::from_str(line).unwrap();
    for like in like_by_mes {
        ids.push(like.id);
    }
    ids
}

#[test]
fn parse_time() {
    // use endpoint::kv::uuid;
    let uuid = 5100423841317698 as i64;
    let seconds = uuid.unix_secs();

    use chrono_tz::Asia::Shanghai;
    let t = chrono::Utc
        .timestamp_opt(seconds, 0)
        .unwrap()
        .with_timezone(&Shanghai)
        .naive_local();
    println!("{}", t);
}
