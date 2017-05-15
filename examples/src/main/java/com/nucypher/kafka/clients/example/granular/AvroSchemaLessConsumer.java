package com.nucypher.kafka.clients.example.granular;

import com.google.common.io.Resources;
import com.nucypher.kafka.DefaultProvider;
import com.nucypher.kafka.TestConstants;
import com.nucypher.kafka.clients.decrypt.AesStructuredMessageDeserializer;
import com.nucypher.kafka.clients.example.utils.JaasUtils;
import com.nucypher.kafka.clients.granular.DataFormat;
import com.nucypher.kafka.utils.KeyUtils;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.HdrHistogram.Histogram;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.File;
import java.io.InputStream;
import java.security.PrivateKey;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

/**
 * Avro granular consumer
 */
public class AvroSchemaLessConsumer {

    public static void main(String[] args) throws Exception {
        DefaultProvider.initializeProvider();
        JaasUtils.initializeConfiguration();

        Histogram stats = new Histogram(1, 10000000, 2);
        // and the consumer
        KafkaConsumer<String, Object> consumer;
        try (InputStream props = Resources.getResource("consumer.properties").openStream()) {
            Properties properties = new Properties();
            properties.load(props);
            if (properties.getProperty("group.id") == null) {
                properties.setProperty("group.id", "group-" + new Random().nextInt(100000));
            }
            properties.put("security.protocol", SecurityProtocol.SASL_PLAINTEXT.toString());
            properties.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
            properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

            // load PEM file from resources
            File file = new File(AvroSchemaLessConsumer.class.getClassLoader()
                    .getResource(TestConstants.PEM).getFile());
            final PrivateKey privateKey = KeyUtils.getECKeyPairFromPEM(file.getAbsolutePath()).getPrivate();

            Deserializer<Object> deserializer = new AesStructuredMessageDeserializer<>(
                    new KafkaAvroDeserializer(),
                    privateKey,
                    DataFormat.AVRO_SCHEMA_LESS
            );
            Map<String, Object> configs = new HashMap<>();
            for (final String name: properties.stringPropertyNames()) {
                configs.put(name, properties.getProperty(name));
            }
            deserializer.configure(configs, false);
            consumer = new KafkaConsumer<>(
                    properties,
                    new StringDeserializer(),
                    deserializer
            );
        }

        consumer.subscribe(Collections.singletonList("granular-avro-schema-less"));
        int timeouts = 0;
        //noinspection InfiniteLoopStatement
        while (true) {
//            Thread.sleep(200);
            // read records with a short timeout. If we time out, we don't really care.
            ConsumerRecords<String, Object> records = consumer.poll(200);
            System.out.println("records.count():" + records.count());
            if (records.count() == 0) {
                timeouts++;
            } else {
                System.out.printf("Got %d records after %d timeouts\n", records.count(), timeouts);
                timeouts = 0;
            }
            for (ConsumerRecord<String, Object> record : records) {
                GenericRecord genericRecord = (GenericRecord) record.value();
//                System.out.println(genericRecord);
                // the send time is encoded inside the message
                long latency = (long) ((System.nanoTime() * 1e-9 - (double) genericRecord.get("t")) * 1000);
                stats.recordValue(latency);
            }
        }
    }
}

