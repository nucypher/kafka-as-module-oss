package com.nucypher.kafka.clients.message;

import com.nucypher.kafka.DefaultProvider;
import com.nucypher.kafka.TestConstants;
import com.nucypher.kafka.clients.Message;
import com.nucypher.kafka.clients.decrypt.aes.AesGcmCipherDecryptor;
import com.nucypher.kafka.clients.encrypt.aes.AesGcmCipherEncryptor;
import com.nucypher.kafka.utils.KeyUtils;
import com.nucypher.kafka.utils.MessageUtils;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.io.File;
import java.security.PrivateKey;
import java.security.PublicKey;

/**
 *
 */
public class MessageTest {

    @Test
    public void testMessage() throws Exception {
        DefaultProvider.initializeProvider();

        File file = new File(MessageTest.class.getClassLoader()
                .getResource(TestConstants.PEM).getFile());
        final PublicKey publicKey = KeyUtils.getECKeyPairFromPEM(file.getAbsolutePath()).getPublic();
        final PrivateKey privateKey = KeyUtils.getECKeyPairFromPEM(file.getAbsolutePath()).getPrivate();

        AesGcmCipherEncryptor aesGcmEncryptor =
                new AesGcmCipherEncryptor(TestConstants.ENCRYPTION_ALGORITHM, publicKey);

        String topic = "TOPIC";
        String payload = "PAYLOAD_TEXT";

        // for AES we need to save and IV in message
        byte[] encryptedMessageBytes = new Message<>(
                new StringSerializer(),
                aesGcmEncryptor,
                MessageUtils.getHeader(topic, aesGcmEncryptor),
                payload).encrypt();

        AesGcmCipherDecryptor aesGcmDecryptor = new AesGcmCipherDecryptor(privateKey);
        Message<String> decryptedMessage = new Message<>(new StringDeserializer(), aesGcmDecryptor, encryptedMessageBytes);

        String backPayload = decryptedMessage.decrypt();
        System.out.println("backPayload:" + backPayload);

        assert payload.equals(backPayload);
    }
}
