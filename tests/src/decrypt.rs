use base64::{engine::general_purpose, Engine as _};
use ds::decrypt::decrypt_password;

/// 测试场景：正常&异常解密密码
/// 特征：预先通过公钥加密密码，转换为base64编码的字符串，然后通过私钥解密，得到原始密码
/// 测试步骤：
///     1.将公钥加密后的base64转为字节数据
///     2.非base64编码的数据
///     3.base64编码的数据，但不是加密数据
///     4.验证解密结果是否符合预期
#[test]
fn test_decrypt_password() {
    let key_pem = "-----BEGIN PRIVATE KEY-----
MIIBVgIBADANBgkqhkiG9w0BAQEFAASCAUAwggE8AgEAAkEA40P3jO4LhEu4NP8Z
kT8XXJiI5zybcVqw/+WpZhAteQJ1YgzhjNP3YGMXtemYT/cwO91ro2ogGReZZaQG
3SNOlQIDAQABAkEAqshrohNMwkkoj2LYcsbnpmTWFHb+FOvjMRoD97fWhCTCxLAq
MTcrGNc43jJ9dKC29IEdWv7YaaNEIKmgAuAF4QIhAP6rqrGznxeBGUyPB3Hl6RD9
/DwMBcytnfuP3TNPem2NAiEA5HOtT2Nuj6UkE+2OomdB+Znv6wCQQgOf2CsRZlAu
jykCIC+3DEFFLT6jIpFUjwmJERTs8XBytDd4JAx5FPHDJ2YVAiEAjXWHmoICYxYp
+eD+kleIBcupQQYvTYE7CDra4lTCD8kCIQCSju4GL3WOrN+R0J97IM8ZBbfsGXPL
R7fqrcMo7Htwzw==
-----END PRIVATE KEY-----"
        .to_string();

    // 正常情况下的加密数据
    let data =
        "tKVADH4Nyn+vQFOco1RT3KV9TC4RJj3viGf1VWGaTiBQSpg3+vy/VDhOxb7WEZETha8MIRD0/raYEkNAo4TOXA=="
            .to_string();
    let encrypted_data = general_purpose::STANDARD
        .decode(data.as_bytes())
        .expect("INVALID_PASSWORD");

    let result = decrypt_password(&key_pem, &encrypted_data);
    assert!(result.is_ok());
    let decrypted_password = result.expect("ok");
    assert_eq!(decrypted_password, b"xYsA0daSCDAEsmMDA0MDA");

    // 异常加密数据: 非base64编码的数据
    let data = "INVALID_DATA".to_string();
    let encrypted_data = general_purpose::STANDARD.decode(data.as_bytes());
    assert!(encrypted_data.is_err());

    // 异常加密数据: base64编码的数据，但不是加密数据
    let data =
        "rLZg/70OiukQca4c4O0FQRlI7daa/bEP++wovp375aUi3imp+D/wCMMiQe38a31uBFoRePtqT5alVmR8Ifs2TA=="
            .to_string();
    let encrypted_data = general_purpose::STANDARD
        .decode(data.as_bytes())
        .expect("INVALID_PASSWORD");

    let result = decrypt_password(&key_pem, &encrypted_data);
    assert!(result.is_ok());
    assert_ne!(result.expect("ok"), b"xYsA0daSCDAEsmMDA0MDA")
}

/// 测试场景：无效数据
/// 特征：空的加密数据，无效的私钥，空的私钥
/// 测试步骤：
///     1.空的加密数据
///     2.无效的私钥
///     3.空的私钥
///     4.验证解密结果是否符合预期
#[test]
fn invalid_data() {
    let key_pem = "-----BEGIN PRIVATE KEY-----
MIIBVgIBADANBgkqhkiG9w0BAQEFAASCAUAwggE8AgEAAkEA40P3jO4LhEu4NP8Z
kT8XXJiI5zybcVqw/+WpZhAteQJ1YgzhjNP3YGMXtemYT/cwO91ro2ogGReZZaQG
3SNOlQIDAQABAkEAqshrohNMwkkoj2LYcsbnpmTWFHb+FOvjMRoD97fWhCTCxLAq
MTcrGNc43jJ9dKC29IEdWv7YaaNEIKmgAuAF4QIhAP6rqrGznxeBGUyPB3Hl6RD9
/DwMBcytnfuP3TNPem2NAiEA5HOtT2Nuj6UkE+2OomdB+Znv6wCQQgOf2CsRZlAu
jykCIC+3DEFFLT6jIpFUjwmJERTs8XBytDd4JAx5FPHDJ2YVAiEAjXWHmoICYxYp
+eD+kleIBcupQQYvTYE7CDra4lTCD8kCIQCSju4GL3WOrN+R0J97IM8ZBbfsGXPL
R7fqrcMo7Htwzw==
-----END PRIVATE KEY-----"
        .to_string();

    // 边界条件 1: 空的加密数据
    let encrypted_data: Vec<u8> = vec![];
    let result = decrypt_password(&key_pem, &encrypted_data);
    assert!(result.is_err());

    // 边界条件 2: 无效的私钥
    let encrypted_data: Vec<u8> = vec![0, 1, 2, 3];
    let result = decrypt_password(&"INVALID_KEY".to_string(), &encrypted_data);
    assert!(result.is_err());

    // 边界条件 3: 空的私钥
    let encrypted_data: Vec<u8> = vec![0, 1, 2, 3];
    let result = decrypt_password(&"".to_string(), &encrypted_data);
    assert!(result.is_err());
}