use openssl::pkey::{PKey};
use openssl::rsa::Padding;

// key_pem: rsa私钥,pem格式
// encrypted_data: 加密数据
pub fn decrypt_password(key_pem: String, encrypted_data: Vec<u8>) -> Result<String, Box<dyn std::error::Error>> {
    let private_key = PKey::private_key_from_pem(key_pem.as_bytes())?;
    let rsa = private_key.rsa()?;
    // 缓冲区大小必须 >= 密钥长度
    let mut decrypted_data = vec![0; rsa.size() as usize];
    rsa.private_decrypt(&encrypted_data, &mut decrypted_data, Padding::PKCS1)?;
    // 去除多余的0，只保留原始数据
    decrypted_data.retain(|&x| x != 0);
    Ok(String::from_utf8(decrypted_data).expect("invalid utf8 string"))
}