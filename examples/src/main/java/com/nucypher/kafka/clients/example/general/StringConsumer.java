package com.nucypher.kafka.clients.example.general;

import com.nucypher.kafka.TestUtils;
import com.nucypher.kafka.clients.decrypt.AesMessageDeserializer;
import com.nucypher.kafka.clients.decrypt.AesStructuredMessageDeserializer;
import com.nucypher.kafka.clients.example.utils.JaasUtils;
import com.nucypher.kafka.clients.granular.DataFormat;
import com.nucypher.kafka.errors.CommonException;
import com.nucypher.kafka.utils.KeyUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.security.PrivateKey;
import java.util.Collections;
import java.util.Properties;

/**
 * Consumer for receiving batch of string messages
 */
public class StringConsumer {

    /**
     * Send one message
     *
     * @param args args[0] - type. NON, FULL, GRANULAR
     *             args[1] - topic/channel name
     *             args[2] - private key path (for full and partial types)
     */
    public static void main(String[] args) throws Exception {
        JaasUtils.initializeConfiguration();

        if (args.length < 2) {
            System.out.println("Usage: <type> <topic> [<private key>]");
            System.out.println("<type> - type. NON, FULL, GRANULAR");
            System.out.println("<topic> - topic/channel name");
            System.out.println("<private key> - private key path (for full and granular types)");
            return;
        }

        String type = args[0];
        String topic = args[1];
        String keyPath = args.length > 2 ? args[2] : null;

        try (KafkaConsumer<String, String> consumer = getConsumer(type, keyPath)) {
            consumer.subscribe(Collections.singletonList(topic));
            ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("Key = %s, Value = %s\n", record.key(), record.value());
            }
        }
    }

    private static KafkaConsumer<String, String> getConsumer(
            String type, String keyPath) throws IOException {
        PrivateKey privateKey;
        switch (type.toLowerCase()) {
            case "non":
                return new KafkaConsumer<>(
                        getProperties(),
                        new StringDeserializer(),
                        new StringDeserializer()
                );
            case "full":
                privateKey = KeyUtils.getECKeyPairFromPEM(keyPath).getPrivate();
                return new KafkaConsumer<>(
                        getProperties(),
                        new StringDeserializer(),
                        new AesMessageDeserializer<>(
                                new StringDeserializer(),
                                TestUtils.ENCRYPTION_ALGORITHM_CLASS,
                                privateKey
                        )
                );
            case "granular":
                privateKey = KeyUtils.getECKeyPairFromPEM(keyPath).getPrivate();
                return new KafkaConsumer<>(
                        getProperties(),
                        new StringDeserializer(),
                        new AesStructuredMessageDeserializer<>(
                                new StringDeserializer(),
                                TestUtils.ENCRYPTION_ALGORITHM_CLASS,
                                privateKey,
                                DataFormat.JSON
                        )
                );
            default:
                throw new CommonException("Wrong type '%s'", type);
        }

    }

    private static Properties getProperties() {
        Properties consumerProps = new Properties();
        consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "group");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put("security.protocol", SecurityProtocol.SASL_PLAINTEXT.toString());
        consumerProps.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        return consumerProps;
    }

}
