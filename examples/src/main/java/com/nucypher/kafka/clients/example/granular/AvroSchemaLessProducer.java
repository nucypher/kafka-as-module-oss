package com.nucypher.kafka.clients.example.granular;

import com.google.common.io.Resources;
import com.nucypher.kafka.TestUtils;
import com.nucypher.kafka.clients.encrypt.AesStructuredMessageSerializer;
import com.nucypher.kafka.clients.example.utils.JaasUtils;
import com.nucypher.kafka.clients.granular.DataFormat;
import com.nucypher.kafka.utils.KeyUtils;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.io.InputStream;
import java.security.PublicKey;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * JSON granular producer
 */
public class AvroSchemaLessProducer {

    public static void main(String[] args) throws Exception {
        Schema schema = SchemaBuilder.record("record")
                .fields()
                .name("type").type().stringType().stringDefault("test")
                .name("t").type().doubleType().noDefault()
                .name("k").type().intType().noDefault()
                .endRecord();

        JaasUtils.initializeConfiguration();

        // set up the producer
        KafkaProducer<String, Object> producer;
        try (InputStream props = Resources.getResource("producer.properties").openStream()) {
            Properties properties = new Properties();
            properties.load(props);
            properties.put("security.protocol", SecurityProtocol.SASL_PLAINTEXT.toString());
            properties.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
            properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

            // load PEM file from resources
            File file = new File(AvroSchemaLessProducer.class.getClassLoader()
                    .getResource(TestUtils.PEM).getFile());
            PublicKey publicKey = KeyUtils.getECKeyPairFromPEM(file.getAbsolutePath()).getPublic();

            Serializer<Object> serializer = new AesStructuredMessageSerializer<>(
                    new KafkaAvroSerializer(),
                    TestUtils.ENCRYPTION_ALGORITHM_CLASS,
                    publicKey,
                    DataFormat.AVRO_SCHEMA_LESS
            );
            Map<String, Object> configs = new HashMap<>();
            for (final String name : properties.stringPropertyNames()) {
                configs.put(name, properties.getProperty(name));
            }
            serializer.configure(configs, false);
            producer = new KafkaProducer<>(
                    properties,
                    new StringSerializer(),
                    serializer
            );
        }
        try {
            GenericRecord record = new GenericData.Record(schema);
            for (int i = 0; i < 1000000; i++) {
                record.put("type", "test");
                record.put("t", System.nanoTime() * 1e-9);
                record.put("k", i);
                // send lots of messages
                producer.send(new ProducerRecord<>("granular-avro-schema-less", record));
                Thread.sleep(1000);
            }
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        } finally {
            producer.close();
        }
    }
}
