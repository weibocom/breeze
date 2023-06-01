use openssl::pkey::PKey;
use openssl::rsa::Padding;

// key_pem: rsa私钥,pem格式
// encrypted_data: 加密数据
pub fn decrypt_password(
    key_pem: &String,
    encrypted_data: &Vec<u8>,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let private_key = PKey::private_key_from_pem(key_pem.as_bytes())?;
    let rsa = private_key.rsa()?;
    // 缓冲区大小必须 >= 密钥长度
    let mut decrypted_data = Vec::new();
    decrypted_data.resize(rsa.size() as usize, 0);
    rsa.private_decrypt(&encrypted_data, &mut decrypted_data, Padding::PKCS1)?;
    // 去除多余的0，只保留原始数据
    decrypted_data.retain(|&x| x != 0);
    Ok(decrypted_data)
}