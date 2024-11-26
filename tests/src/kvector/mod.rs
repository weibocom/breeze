use std::{fs::File, io::BufRead};

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
    let file_name = "datas/consistency.log";
    let file = File::open(file_name).unwrap();
    let reader = std::io::BufReader::new(file);

    let mut mesh_ids = Vec::new();
    let mut tcp_ids = Vec::new();
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
            tcp_ids = ids;
            // println!("tcp_ids: {:?}", tcp_ids);

            let diff_in_mesh: Vec<usize> = mesh_ids
                .clone()
                .into_iter()
                .filter(|id| !tcp_ids.contains(id))
                .collect();
            let diff_in_tcp: Vec<usize> = tcp_ids
                .clone()
                .into_iter()
                .filter(|id| !mesh_ids.contains(id))
                .collect();

            if diff_in_mesh.len() > 0 || diff_in_tcp.len() > 0 {
                println!("raw tcp line:{}", line);
                println!("mesh more ids:{:?}", diff_in_mesh);
                println!("tcp more ids: {:?}", diff_in_tcp);

                count += 1;
                if count >= MAX_COUNT {
                    break;
                }

                mesh_ids.clear();
                tcp_ids.clear();
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
