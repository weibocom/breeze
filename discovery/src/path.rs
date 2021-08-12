use std::path::PathBuf;
// 一个标准的unix socket path如下
// /tmp/breeze/socks/config+v1+breeze+feed.content.icy:user@mc@cs.sock
pub struct Path;

impl Path {
    // 第一个部分是biz
    // 第二个部分是资源类型
    // 第三个部分是discovery类型
    pub fn parse(path: &String) -> Option<(String, String, String)> {
        let base = Self::file_name(&path);
        let base = Self::file_name(&base.replace("+", "/"));
        let fields: Vec<String> = base.split("@").map(|e| e.to_string()).collect();
        if fields.len() == 3 {
            // 只取':'后面的作为biz
            let idx = fields[0].find(':').map(|idx| idx + 1).unwrap_or(0);
            Some((
                fields[0][idx..].to_string(),
                fields[1].clone(),
                fields[2].clone(),
            ))
        } else {
            None
        }
    }

    fn file_name(name: &str) -> String {
        PathBuf::from(name)
            .file_name()
            .expect("valid file name")
            .to_str()
            .expect("not utf8 name")
            .to_string()
    }
}
