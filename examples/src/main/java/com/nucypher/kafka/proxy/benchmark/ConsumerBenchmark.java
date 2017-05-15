package com.nucypher.kafka.proxy.benchmark;

import com.nucypher.kafka.clients.decrypt.AesStructuredMessageDeserializer;
import com.nucypher.kafka.clients.decrypt.AesStructuredMessageDeserializerConfig;
import com.nucypher.kafka.clients.granular.JsonDataAccessor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Consumer-proxy benchmark. Run Kafka and Proxy before benchmarking
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Measurement(iterations = 100)
public class ConsumerBenchmark {

    public static final String TOPIC = "benchmark";

    private KafkaConsumer<String, String> consumer;

    @Param({"localhost:9092;false", "localhost:9092;true", "localhost:9192;false"})
    public String parameters;

    private long count;

    @Setup
    public void setup() {
        String[] parts = parameters.split(";");
        String server = parts[0];
        boolean clientEncryption = Boolean.parseBoolean(parts[1]);
        System.setProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM, "src/main/resources/jaas.conf");
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, server + "-" + UUID.randomUUID().toString());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put("security.protocol", SecurityProtocol.SASL_PLAINTEXT.toString());
        properties.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        if (clientEncryption) {
            properties.put(AesStructuredMessageDeserializerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    properties.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG));
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    AesStructuredMessageDeserializer.class);
            properties.put(AesStructuredMessageDeserializerConfig.GRANULAR_DATA_ACCESSOR_CONFIG,
                    JsonDataAccessor.class);
            properties.put(AesStructuredMessageDeserializerConfig.PRIVATE_KEY_CONFIG,
                    "src/main/resources/P521.pem");
        }
        consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singleton(TOPIC));
    }

    @TearDown
    public void close() {
        consumer.close();
        System.out.println("Count = " + count);
    }

    @Benchmark
    public void testConsumer(Blackhole blackhole) {
        ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
        for (ConsumerRecord<String, String> record : records) {
            blackhole.consume(record.value());
        }
        count += records.count();
    }

    public static void main(String[] args) throws Exception {
        Options options = new OptionsBuilder()
                .include(ConsumerBenchmark.class.getCanonicalName())
                .forks(1)
                .build();

        new Runner(options).run();
    }
}
