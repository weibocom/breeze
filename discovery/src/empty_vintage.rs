pub struct Vintage {}

use url::Url;

impl Vintage {
    pub fn from_url(_url: Url) -> Self {
        Self {}
    }
    pub fn get_service(&self, _name: &str) -> String {
        // 两组l1, 一主一从
        //"127.0.0.1:11211;127.0.0.1:11212,127.0.0.1:11213;127.0.0.1:11214;127.0.0.1:11215;"
        "127.0.0.1:11211".to_owned()
    }
}
