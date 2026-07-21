package com.company.pipeline.common.crypto;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.security.crypto.encrypt.Encryptors;
import org.springframework.security.crypto.encrypt.TextEncryptor;
import org.springframework.stereotype.Service;

/**
 * pipeline.crypto.secret/salt(PIPELINE_CRYPTO_SECRET/PIPELINE_CRYPTO_SALT 환경변수)로
 * pipeline_connection.encrypted_password를 AES-256/GCM으로 암복호화한다. 두 값 모두
 * 기본값이 없어 미설정 시 부팅이 실패한다(하드코딩 키 방지).
 */
@Service
@EnableConfigurationProperties(CryptoProperties.class)
public class AesPasswordCryptoService implements PasswordCryptoService {

    private final TextEncryptor textEncryptor;

    public AesPasswordCryptoService(CryptoProperties properties) {
        this.textEncryptor = Encryptors.delux(properties.secret(), properties.salt());
    }

    @Override
    public String encrypt(String plaintext) {
        return textEncryptor.encrypt(plaintext);
    }

    @Override
    public String decrypt(String ciphertext) {
        return textEncryptor.decrypt(ciphertext);
    }
}
