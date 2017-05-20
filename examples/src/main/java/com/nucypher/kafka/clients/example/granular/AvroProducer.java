package com.nucypher.kafka.clients.example.granular;

import com.google.common.io.Resources;
import com.nucypher.kafka.TestConstants;
import com.nucypher.kafka.clients.encrypt.AesStructuredMessageSerializer;
import com.nucypher.kafka.clients.example.utils.JaasUtils;
import com.nucypher.kafka.clients.granular.DataFormat;
import com.nucypher.kafka.utils.KeyUtils;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.security.PublicKey;
import java.util.Properties;

/**
 * JSON granular producer
 */
public class AvroProducer {

    public static void main(String[] args) throws Exception {
        Schema schema = SchemaBuilder.record("record")
                .fields()
                .name("type").type().stringType().stringDefault("test")
                .name("t").type().doubleType().noDefault()
                .name("k").type().intType().noDefault()
                .endRecord();

        JaasUtils.initializeConfiguration();

        KafkaProducer<String, byte[]> producer;
        try (InputStream props = Resources.getResource("producer.properties").openStream()) {
            Properties properties = new Properties();
            properties.load(props);
            properties.put("security.protocol", SecurityProtocol.SASL_PLAINTEXT.toString());
            properties.put(SaslConfigs.SASL_MECHANISM, "PLAIN");

            File file = new File(AvroProducer.class.getClassLoader()
                    .getResource(TestConstants.PEM).getFile());
            PublicKey publicKey = KeyUtils.getECKeyPairFromPEM(file.getAbsolutePath()).getPublic();

            producer = new KafkaProducer<>(
                    properties,
                    new StringSerializer(),
                    new AesStructuredMessageSerializer<>(
                            new ByteArraySerializer(),
                            TestConstants.ENCRYPTION_ALGORITHM,
                            publicKey,
                            DataFormat.AVRO
                    )
            );
        }
        try {
            GenericRecord record = new GenericData.Record(schema);

            for (int i = 0; i < 1000000; i++) {
                record.put("type", "test");
                record.put("t", System.nanoTime() * 1e-9);
                record.put("k", i);
                ByteArrayOutputStream dataOutputStream = new ByteArrayOutputStream();
                DatumWriter<GenericRecord> writer = new GenericDatumWriter<>();
                DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(writer);
                dataFileWriter.create(schema, dataOutputStream);
                dataFileWriter.append(record);
                dataFileWriter.close();
                byte[] bytes = dataOutputStream.toByteArray();

                // send lots of messages
                producer.send(new ProducerRecord<>("granular-avro", bytes));
                Thread.sleep(1000);
            }
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        } finally {
            producer.close();
        }
    }
}
