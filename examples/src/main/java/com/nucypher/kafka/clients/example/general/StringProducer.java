package com.nucypher.kafka.clients.example.general;

import com.nucypher.kafka.TestUtils;
import com.nucypher.kafka.clients.encrypt.AesMessageSerializer;
import com.nucypher.kafka.clients.encrypt.AesStructuredMessageSerializer;
import com.nucypher.kafka.clients.example.utils.JaasUtils;
import com.nucypher.kafka.clients.granular.DataFormat;
import com.nucypher.kafka.errors.CommonException;
import com.nucypher.kafka.utils.KeyUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.security.PublicKey;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * Producer for sending only one string message
 */
public class StringProducer {

    /**
     * Send one message
     *
     * @param args args[0] - type. NON, FULL, GRANULAR
     *             args[1] - topic/channel name
     *             args[2] - message
     *             args[3] - public key path (for full and partial types)
     *             args[4+] - field names for granular encryption (for partial type)
     */
    public static void main(String[] args) throws Exception {
        JaasUtils.initializeConfiguration();

        if (args.length < 3) {
            System.out.println("Usage: <type> <topic> <message> [<public key> [<field>...]]");
            System.out.println("<type> - type. NON, FULL, GRANULAR");
            System.out.println("<topic> - topic/channel name");
            System.out.println("<message> - message");
            System.out.println("<public key> - public key path (for full and partial types)");
            System.out.println("<field>... - field names for granular encryption (for granular type)");
            return;
        }

        String type = args[0];
        String topic = args[1];
        String message = args[2];
        String keyPath = args.length > 3 ? args[3] : null;
        Set<String> fields = null;
        if (args.length > 4) {
            fields = new HashSet<>(Arrays.asList(Arrays.copyOfRange(args, 4, args.length)));
        }

        String key = "key";
        try (KafkaProducer<String, String> producer = getProducer(type, keyPath, fields)) {
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>(topic, key, message);
            producer.send(producerRecord).get();
        }
        System.out.println("Done");
    }

    private static KafkaProducer<String, String> getProducer(
            String type, String keyPath, Set<String> fields) throws IOException {
        PublicKey publicKey;
        switch (type.toLowerCase()) {
            case "non":
                return new KafkaProducer<>(
                        getProperties(),
                        new StringSerializer(),
                        new StringSerializer()
                );
            case "full":
                publicKey = KeyUtils.getECKeyPairFromPEM(keyPath).getPublic();
                return new KafkaProducer<>(
                        getProperties(),
                        new StringSerializer(),
                        new AesMessageSerializer<>(
                                new StringSerializer(),
                                TestUtils.ENCRYPTION_ALGORITHM_CLASS,
                                publicKey,
                                null
                        )
                );
            case "granular":
                publicKey = KeyUtils.getECKeyPairFromPEM(keyPath).getPublic();
                return new KafkaProducer<>(
                        getProperties(),
                        new StringSerializer(),
                        new AesStructuredMessageSerializer<>(
                                new StringSerializer(),
                                TestUtils.ENCRYPTION_ALGORITHM_CLASS,
                                publicKey,
                                null,
                                DataFormat.JSON,
                                fields
                        )
                );
            default:
                throw new CommonException("Wrong type '%s'", type);
        }

    }

    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put("security.protocol", SecurityProtocol.SASL_PLAINTEXT.toString());
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        return props;
    }

}
