package com.nucypher.kafka.cipher;

import com.nucypher.kafka.ByteTranslator;
import com.nucypher.kafka.StreamTranslator;

import java.security.Key;
import java.security.PrivateKey;
import java.security.PublicKey;

/**
 *
 */
public interface ICipher extends ByteTranslator, StreamTranslator {
    PrivateKey getPrivateKey();
    PublicKey getPublicKey();
    Key getKey();
    byte[] getIV();
    CryptType getCryptType();
    ICipher init(Key key, byte[] IV);
}
