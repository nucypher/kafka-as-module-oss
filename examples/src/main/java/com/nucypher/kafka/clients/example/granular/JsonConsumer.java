package com.nucypher.kafka.clients.example.granular;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;
import com.nucypher.kafka.TestUtils;
import com.nucypher.kafka.clients.decrypt.AesStructuredMessageDeserializer;
import com.nucypher.kafka.clients.example.utils.JaasUtils;
import com.nucypher.kafka.clients.granular.DataFormat;
import com.nucypher.kafka.utils.KeyUtils;
import org.HdrHistogram.Histogram;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.File;
import java.io.InputStream;
import java.security.PrivateKey;
import java.util.Collections;
import java.util.Properties;
import java.util.Random;

/**
 * JSON granular consumer
 */
public class JsonConsumer {

    public static void main(String[] args) throws Exception {
        JaasUtils.initializeConfiguration();

        ObjectMapper mapper = new ObjectMapper();
        Histogram stats = new Histogram(1, 10000000, 2);
        // and the consumer
        KafkaConsumer<String, String> consumer;
        try (InputStream props = Resources.getResource("consumer.properties").openStream()) {
            Properties properties = new Properties();
            properties.load(props);
            if (properties.getProperty("group.id") == null) {
                properties.setProperty("group.id", "group-" + new Random().nextInt(100000));
            }
            properties.put("security.protocol", SecurityProtocol.SASL_PLAINTEXT.toString());
            properties.put(SaslConfigs.SASL_MECHANISM, "PLAIN");

            // load PEM file from resources
            File file = new File(JsonConsumer.class.getClassLoader()
                    .getResource(TestUtils.PEM).getFile());
            final PrivateKey privateKey =
                    KeyUtils.getECKeyPairFromPEM(file.getAbsolutePath()).getPrivate();

            consumer = new KafkaConsumer<>(
                    properties,
                    new StringDeserializer(),
                    new AesStructuredMessageDeserializer<>(
                            new StringDeserializer(),
                            TestUtils.ENCRYPTION_ALGORITHM_CLASS,
                            privateKey,
                            DataFormat.JSON
                    )
            );

        }

        consumer.subscribe(Collections.singletonList("granular-json"));
        int timeouts = 0;
        //noinspection InfiniteLoopStatement
        while (true) {
//            Thread.sleep(200);
            // read records with a short timeout. If we time out, we don't really care.
            ConsumerRecords<String, String> records = consumer.poll(200);
            System.out.println("records.count():" + records.count());
            if (records.count() == 0) {
                timeouts++;
            } else {
                System.out.printf("Got %d records after %d timeouts\n", records.count(), timeouts);
                timeouts = 0;
            }
            for (ConsumerRecord<String, String> record : records) {
                // the send time is encoded inside the message
                JsonNode message = mapper.readTree(record.value());
                long latency = (long) ((System.nanoTime() * 1e-9 - message.get("t").asDouble()) * 1000);
                stats.recordValue(latency);
            }
        }
    }
}

