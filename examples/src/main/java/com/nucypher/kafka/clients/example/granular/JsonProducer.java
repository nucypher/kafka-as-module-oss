package com.nucypher.kafka.clients.example.granular;

import com.google.common.io.Resources;
import com.nucypher.kafka.DefaultProvider;
import com.nucypher.kafka.TestConstants;
import com.nucypher.kafka.clients.encrypt.AesStructuredMessageSerializer;
import com.nucypher.kafka.clients.example.utils.JaasUtils;
import com.nucypher.kafka.clients.granular.DataFormat;
import com.nucypher.kafka.utils.KeyUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.io.InputStream;
import java.security.PublicKey;
import java.util.Locale;
import java.util.Properties;

/**
 * JSON granular producer
 */
public class JsonProducer {

    public static void main(String[] args) throws Exception {
        DefaultProvider.initializeProvider();
        JaasUtils.initializeConfiguration();

        // set up the producer
        KafkaProducer<String, String> producer;
        try (InputStream props = Resources.getResource("producer.properties").openStream()) {
            Properties properties = new Properties();
            properties.load(props);
            properties.put("security.protocol", SecurityProtocol.SASL_PLAINTEXT.toString());
            properties.put(SaslConfigs.SASL_MECHANISM, "PLAIN");

            // load PEM file from resources
            File file = new File(JsonProducer.class.getClassLoader()
                    .getResource(TestConstants.PEM).getFile());
            PublicKey publicKey = KeyUtils.getECKeyPairFromPEM(file.getAbsolutePath()).getPublic();

            producer = new KafkaProducer<>(
                    properties,
                    new StringSerializer(),
                    new AesStructuredMessageSerializer<>(
                            new StringSerializer(),
                            TestConstants.ENCRYPTION_ALGORITHM,
                            publicKey,
                            DataFormat.JSON
                    )
            );

        }
        try {
            for (int i = 0; i < 1000000; i++) {
                // send lots of messages
                producer.send(new ProducerRecord<>(
                        "granular-json",
                        String.format(Locale.US,
                                "{\"type\":\"test\", \"t\":%.3f, \"k\":%d}",
                                System.nanoTime() * 1e-9,
                                i)));

                Thread.sleep(1000);
            }
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        } finally {
            producer.close();
        }
    }
}
