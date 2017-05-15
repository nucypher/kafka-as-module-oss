package com.nucypher.kafka.clients.example.general;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;
import com.nucypher.kafka.TestConstants;
import com.nucypher.kafka.clients.decrypt.aes.AesGcmCipherDecryptor;
import com.nucypher.kafka.clients.decrypt.AesMessageDecryptorDeserializer;
import com.nucypher.kafka.utils.KeyUtils;
import org.HdrHistogram.Histogram;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import javax.crypto.NoSuchPaddingException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.security.*;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;

/**
 * Based on MapR example
 * at
 * https://www.mapr.com/blog/getting-started-sample-programs-apache-kafka-09
 */
public class Consumer {

    public static void main(String[] args) throws IOException, InterruptedException, NoSuchAlgorithmException, NoSuchPaddingException, NoSuchProviderException, InvalidAlgorithmParameterException, InvalidKeyException {
        // set up house-keeping
        ObjectMapper mapper = new ObjectMapper();
        Histogram stats = new Histogram(1, 10000000, 2);
        Histogram global = new Histogram(1, 10000000, 2);
        // and the consumer
        KafkaConsumer<String, String> consumer;
        try (InputStream props = Resources.getResource("consumer.properties").openStream()) {
            Properties properties = new Properties();
            properties.load(props);
            if (properties.getProperty("group.id") == null) {
                properties.setProperty("group.id", "group-" + new Random().nextInt(100000));
            }

            ////////////////////////////////////////////////////////
            //
            // #1 - original / without any decryptor
            //
//            consumer = new KafkaConsumer<>(properties);
//            consumer = new KafkaConsumer<String, String>(
//                    properties,
//                    new StringDeserializer(),
//                    new StringDeserializer()
//                    );


            ////////////////////////////////////////////////////////
            //
            // #2 - ByteDecryptor
            //
//            consumer = new KafkaConsumer<String, String>(
//                    properties,
//
//                    new StringDeserializer(),
//
//                    new ByteDecryptorDeserializer<String>(
//                            new StringDeserializer(),
//                            new InverseByteDecryptor()
//                    )
//            );


            //////////////////////////////////////////////////////////
            //
            // #3 - AES Decryptor
            //

            // load PEM file from resources
            File file = new File(Consumer.class.getClassLoader().getResource(TestConstants.PEM).getFile());
            final PrivateKey privateKey = KeyUtils.getECKeyPairFromPEM(file.getAbsolutePath()).getPrivate();

            AesGcmCipherDecryptor aesGcmDecryptor = new AesGcmCipherDecryptor(privateKey);

            consumer = new KafkaConsumer<String, String>(
                    properties,
                    new StringDeserializer(),
                    new AesMessageDecryptorDeserializer<String>(
                            new StringDeserializer(),
                            aesGcmDecryptor
                    )
            );

        }

        consumer.subscribe(Arrays.asList("fast-messages", "summary-markers"));
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
                switch (record.topic()) {
                    case "fast-messages":
                        // the send time is encoded inside the message
                        JsonNode msg = mapper.readTree(record.value());
                        switch (msg.get("type").asText()) {
                            case "test":
                                long latency = (long) ((System.nanoTime() * 1e-9 - msg.get("t").asDouble()) * 1000);
                                stats.recordValue(latency);
                                global.recordValue(latency);
                                break;
                            case "marker":
                                // whenever we get a marker message, we should dump out the stats
                                // note that the number of fast messages won't necessarily be quite constant
                                System.out.printf("%d messages received in period, latency(min, max, avg, 99%%) = %d, %d, %.1f, %d (ms)\n",
                                        stats.getTotalCount(),
                                        stats.getValueAtPercentile(0), stats.getValueAtPercentile(100),
                                        stats.getMean(), stats.getValueAtPercentile(99));
                                System.out.printf("%d messages received overall, latency(min, max, avg, 99%%) = %d, %d, %.1f, %d (ms)\n",
                                        global.getTotalCount(),
                                        global.getValueAtPercentile(0), global.getValueAtPercentile(100),
                                        global.getMean(), global.getValueAtPercentile(99));
                                stats.reset();
                                break;
                            default:
                                throw new IllegalArgumentException("Illegal message type: " + msg.get("type"));
                        }
                        break;
                    case "summary-markers":
                        break;
                    default:
                        throw new IllegalStateException("Shouldn't be possible to get message on topic " + record.topic());
                }
            }
        }
    }

//    public static void main(String... args) throws Exception {
//
//        ObjectMapper mapper = new ObjectMapper();
//
//        Histogram stats = new Histogram(1, 10000000, 2);
//        Histogram global = new Histogram(1, 10000000, 2);
//
//        KafkaConsumer<String, String> consumer;
//
//        try (InputStream inputStreamProperties = Resources.getResource("consumer.properties").openStream()) {
//            Properties properties = new Properties();
//            properties.load(inputStreamProperties);
//            if (properties.getProperty("group.id") == null) {
//                properties.setProperty("group.id", "group-" + new Random().nextInt(100000));
//            }
//
//            // #1
////            consumer = new KafkaConsumer<String, String>(
////                    properties,
////                    new StringDeserializer(),
////                    new ByteDecryptorDeserializer<String>(new StringDeserializer())
////            );
//
//            // #2
////            consumer = new KafkaConsumer<String, String>(
////                    properties,
////                    new StringDeserializer(),
////                    new ByteDecryptorDeserializer<String>(new StringDeserializer()){
////                        private final ByteDecryptor decryptor = new InverseByteDecryptor();
////                        @Override
////                        public ByteDecryptor getByteDecryptor() {
////                            return decryptor;
////                        }
////                    }
////            );
//
//            // #3
////            consumer = new KafkaConsumer<String, String>(
////                    properties,
////                    new StringDeserializer(),
////                    new ByteDecryptorDeserializer<String>(new StringDeserializer(), new InverseByteDecryptor())
////            );
//
//            // #4
//            consumer = new KafkaConsumer<String, String>(
//                    properties,
//                    new StringDeserializer(),
//                    new StringByteDecryptorDeserializer(new InverseByteDecryptor())
//            );
//
//            consumer.subscribe(Arrays.asList("fast-messages", "summary-markers"));
//            int timeouts = 0;
//            //noinspection InfiniteLoopStatement
//            while (true) {
//                // read records with a short timeout. If we time out, we don't really care.
//                ConsumerRecords<String, String> records = consumer.poll(200);
//                if (records.count() == 0) {
//                    timeouts++;
//                } else {
//                    System.out.printf("Got %d records after %d timeouts\n", records.count(), timeouts);
//                    timeouts = 0;
//                }
//                for (ConsumerRecord<String, String> record : records) {
//                    switch (record.topic()) {
//                        case "fast-messages":
//                            // the send time is encoded inside the message
//                            JsonNode msg = mapper.readTree(record.value());
//                            switch (msg.get("type").asText()) {
//                                case "test":
//                                    long latency = (long) ((System.nanoTime() * 1e-9 - msg.get("t").asDouble()) * 1000);
//                                    stats.recordValue(latency);
//                                    global.recordValue(latency);
//                                    break;
//                                case "marker":
//                                    // whenever we get a marker message, we should dump out the stats
//                                    // note that the number of fast messages won't necessarily be quite constant
//                                    System.out.printf("%d messages received in period, latency(min, max, avg, 99%%) = %d, %d, %.1f, %d (ms)\n",
//                                            stats.getTotalCount(),
//                                            stats.getValueAtPercentile(0), stats.getValueAtPercentile(100),
//                                            stats.getMean(), stats.getValueAtPercentile(99));
//                                    System.out.printf("%d messages received overall, latency(min, max, avg, 99%%) = %d, %d, %.1f, %d (ms)\n",
//                                            global.getTotalCount(),
//                                            global.getValueAtPercentile(0), global.getValueAtPercentile(100),
//                                            global.getMean(), global.getValueAtPercentile(99));
//                                    stats.reset();
//                                    break;
//                                default:
//                                    throw new IllegalArgumentException("Illegal message type: " + msg.get("type"));
//                            }
//                            break;
//                        case "summary-markers":
//                            break;
//                        default:
//                            throw new IllegalStateException("Shouldn't be possible to get message on topic " + record.topic());
//                    }
//                }
//            }
//
//        }
//
//    }
}
