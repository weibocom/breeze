use rsa::{pkcs8::DecodePrivateKey, Pkcs1v15Encrypt, RsaPrivateKey};

pub fn decrypt_password(
    key_pem: &String,
    encrypted_data: &Vec<u8>,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    // 从PEM格式解析私钥
    let private_key = RsaPrivateKey::from_pkcs8_pem(key_pem)?;

    // 使用PKCS1填充方式解密
    let decrypted_data = private_key.decrypt(Pkcs1v15Encrypt, encrypted_data)?;

    // 不需要手动去除多余的0，rsa库已经处理好了
    Ok(decrypted_data)
}
