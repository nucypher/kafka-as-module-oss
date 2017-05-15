package com.nucypher.kafka.clients.example.general;

import com.google.common.io.Resources;
import com.nucypher.kafka.TestConstants;
import com.nucypher.kafka.clients.encrypt.AesMessageEncryptorSerializer;
import com.nucypher.kafka.clients.encrypt.aes.AesGcmCipherEncryptor;
import com.nucypher.kafka.utils.KeyUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import javax.crypto.NoSuchPaddingException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PublicKey;
import java.util.Properties;

/**
 *
 */
public class Producer {
    public static void main(String[] args) throws IOException, NoSuchAlgorithmException, NoSuchPaddingException, NoSuchProviderException, InvalidAlgorithmParameterException, InvalidKeyException {
        // set up the producer
        KafkaProducer<String, String> producer;
        try (InputStream props = Resources.getResource("producer.properties").openStream()) {
            Properties properties = new Properties();
            properties.load(props);

            ////////////////////////////////////////////////////////
            //
            // #1 - original / without any Encryptor
            //
//            producer = new KafkaProducer<>(properties);

//            producer = new KafkaProducer<String, String>(
//                    properties,
//                    new StringSerializer(),
//                    new StringSerializer()
//            );


            ////////////////////////////////////////////////////////
            //
            // #2 - ByteEncryptor
            //
//            producer = new KafkaProducer<String, String>(
//                    properties,
//                    new StringSerializer(),
//                    new ByteEncryptorSerializer<String>(
//                            new StringSerializer(),
//                            new InverseByteByteEncryptor()
//                    )
//            );


            //////////////////////////////////////////////////////////
            //
            // #3 - AES Encryptor
            //

            // load PEM file from resources
            File file = new File(Producer.class.getClassLoader()
                    .getResource(TestConstants.PEM).getFile());
            PublicKey publicKey = KeyUtils.getECKeyPairFromPEM(file.getAbsolutePath()).getPublic();

            // initialize Encryptor
            AesGcmCipherEncryptor aesGcmEncryptor = new AesGcmCipherEncryptor(
                    TestConstants.ENCRYPTION_ALGORITHM, publicKey);

            producer = new KafkaProducer<>(
                    properties,
                    new StringSerializer(),
                    new AesMessageEncryptorSerializer<>(
                            new StringSerializer(),
                            aesGcmEncryptor,
                            aesGcmEncryptor
                    )
            );

        }
        try {
            for (int i = 0; i < 1000000; i++) {
                // send lots of messages
                producer.send(new ProducerRecord<>(
                        "fast-messages",
                        String.format("{\"type\":\"test\", \"t\":%.3f, \"k\":%d}", System.nanoTime() * 1e-9, i)));
//                // every so often send to a different topic
//                if (i % 1000 == 0) {
//                    producer.send(new ProducerRecord<String, String>(
//                            "fast-messages",
//                            String.format("{\"type\":\"marker\", \"t\":%.3f, \"k\":%d}", System.nanoTime() * 1e-9, i)));
//                    producer.send(new ProducerRecord<String, String>(
//                            "summary-markers",
//                            String.format("{\"type\":\"other\", \"t\":%.3f, \"k\":%d}", System.nanoTime() * 1e-9, i)));
//                    producer.flush();
//                    System.out.println("Sent msg number " + i);
//                }

                Thread.sleep(1000);
            }
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        } finally {
            producer.close();
        }
    }
}
