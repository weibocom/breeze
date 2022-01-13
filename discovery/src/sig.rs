// 签名包含2个部分。
// 1. 内容的摘要；
// 2. 内容提供方(vintage)的唯一id
#[derive(Default, Debug, PartialEq, Eq)]
pub(crate) struct Sig {
    pub(crate) digest: [u8; 16],
    pub(crate) sig: String,
}
impl Sig {
    pub(crate) fn new(digest: [u8; 16], sig: String) -> Self {
        Self { digest, sig }
    }
    pub(crate) fn serialize(&self) -> String {
        let mut buf = String::with_capacity(32 + 1 + self.sig.len());
        let sig_str = bs58::encode(self.digest).into_string();
        buf.push_str(&sig_str);
        buf.push(' ');
        buf.push_str(&self.sig);
        buf
    }
    pub(crate) fn group_sig(&self) -> &str {
        &self.sig
    }
}

use std::convert::TryFrom;
use std::io::{Error, ErrorKind};
// 反序列化
// 用空格区分为两部分。
// 第一部分：是base58编码的digest.
// 第二部分：sig
impl TryFrom<String> for Sig {
    type Error = std::io::Error;
    fn try_from(ser: String) -> std::result::Result<Self, Self::Error> {
        let (digest, sig) = ser
            .find(' ')
            .map(|idx| (ser[0..idx].to_string(), ser[idx + 1..].to_string()))
            .unwrap_or((ser, String::new()));
        let digest: [u8; 16] = bs58::decode(digest)
            .into_vec()
            .map_err(|e| Error::new(ErrorKind::InvalidData, format!("into vec:{:?}", e)))?
            .try_into()
            .map_err(|e| Error::new(ErrorKind::InvalidData, format!("into sig:{:?}", e)))?;
        Ok(Sig { digest, sig })
    }
}
