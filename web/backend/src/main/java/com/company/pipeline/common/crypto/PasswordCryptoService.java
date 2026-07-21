package com.company.pipeline.common.crypto;

public interface PasswordCryptoService {

    String encrypt(String plaintext);

    String decrypt(String ciphertext);
}
