package com.nucypher.kafka.clients.example.granular;

import com.google.common.io.Resources;
import com.nucypher.kafka.DefaultProvider;
import com.nucypher.kafka.TestConstants;
import com.nucypher.kafka.clients.decrypt.AesStructuredMessageDeserializer;
import com.nucypher.kafka.clients.example.utils.JaasUtils;
import com.nucypher.kafka.clients.granular.DataFormat;
import com.nucypher.kafka.utils.KeyUtils;
import org.HdrHistogram.Histogram;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.File;
import java.io.InputStream;
import java.security.PrivateKey;
import java.util.Collections;
import java.util.Properties;
import java.util.Random;

/**
 * Avro granular consumer
 */
public class AvroConsumer {

    public static void main(String[] args) throws Exception {
        DefaultProvider.initializeProvider();
        JaasUtils.initializeConfiguration();

        Histogram stats = new Histogram(1, 10000000, 2);
        // and the consumer
        KafkaConsumer<String, byte[]> consumer;
        try (InputStream props = Resources.getResource("consumer.properties").openStream()) {
            Properties properties = new Properties();
            properties.load(props);
            if (properties.getProperty("group.id") == null) {
                properties.setProperty("group.id", "group-" + new Random().nextInt(100000));
            }
            properties.put("security.protocol", SecurityProtocol.SASL_PLAINTEXT.toString());
            properties.put(SaslConfigs.SASL_MECHANISM, "PLAIN");

            // load PEM file from resources
            File file = new File(AvroConsumer.class.getClassLoader()
                    .getResource(TestConstants.PEM).getFile());
            final PrivateKey privateKey = KeyUtils.getECKeyPairFromPEM(file.getAbsolutePath()).getPrivate();

            consumer = new KafkaConsumer<>(
                    properties,
                    new StringDeserializer(),
                    new AesStructuredMessageDeserializer<>(
                            new ByteArrayDeserializer(),
                            privateKey,
                            DataFormat.AVRO
                    )
            );

        }

        consumer.subscribe(Collections.singletonList("granular-avro"));
        int timeouts = 0;
        //noinspection InfiniteLoopStatement
        while (true) {
//            Thread.sleep(200);
            // read records with a short timeout. If we time out, we don't really care.
            ConsumerRecords<String, byte[]> records = consumer.poll(200);
            System.out.println("records.count():" + records.count());
            if (records.count() == 0) {
                timeouts++;
            } else {
                System.out.printf("Got %d records after %d timeouts\n", records.count(), timeouts);
                timeouts = 0;
            }
            for (ConsumerRecord<String, byte[]> record : records) {
                SeekableInput seekableInput = new SeekableByteArrayInput(record.value());
                DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
                DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(seekableInput, datumReader);

                GenericRecord genericRecord = dataFileReader.next();
//                System.out.println(genericRecord);
                // the send time is encoded inside the message
                long latency = (long) ((System.nanoTime() * 1e-9 - (double) genericRecord.get("t")) * 1000);
                stats.recordValue(latency);
            }
        }
    }
}

