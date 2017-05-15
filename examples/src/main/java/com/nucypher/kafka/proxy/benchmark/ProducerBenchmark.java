package com.nucypher.kafka.proxy.benchmark;

import com.nucypher.kafka.clients.encrypt.AesStructuredMessageSerializer;
import com.nucypher.kafka.clients.encrypt.AesStructuredMessageSerializerConfig;
import com.nucypher.kafka.clients.granular.JsonDataAccessor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.kafka.common.serialization.StringSerializer;
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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Producer-proxy benchmark. Run Kafka and Proxy before benchmarking
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Measurement(iterations = 100)
@Warmup(iterations = 30)
public class ProducerBenchmark {

    private static final String TOPIC = "benchmark";

    private KafkaProducer<String, String> producer;

    @Param({"localhost:9092;false", "localhost:9092;true", "localhost:9192;false"})
    public String parameters;

    @Setup
    public void setup() {
        String[] parts = parameters.split(";");
        String server = parts[0];
        boolean clientEncryption = Boolean.parseBoolean(parts[1]);
        System.setProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM, "src/main/resources/jaas.conf");
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put("security.protocol", SecurityProtocol.SASL_PLAINTEXT.toString());
        properties.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        if (clientEncryption) {
            properties.put(AesStructuredMessageSerializerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                    properties.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
            properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                    AesStructuredMessageSerializer.class);
            properties.put(AesStructuredMessageSerializerConfig.GRANULAR_DATA_ACCESSOR_CONFIG,
                    JsonDataAccessor.class);
            properties.put(AesStructuredMessageSerializerConfig.PUBLIC_KEY_CONFIG,
                    "src/main/resources/P521.pem");
            properties.put(AesStructuredMessageSerializerConfig.FIELDS_LIST_CONFIG,
                    Collections.singletonList("a"));
        }
        producer = new KafkaProducer<>(properties);
    }

    @TearDown
    public void close() {
        producer.close();
    }

    @Benchmark
    public void testProducer() {
        long time = System.nanoTime();
        producer.send(new ProducerRecord<>(TOPIC,
                "{\"a\":" + time +
                        ",\"b\":" + time + "}"));
    }

    @Benchmark
    public void testProducerSync() throws ExecutionException, InterruptedException {
        long time = System.nanoTime();
        producer.send(new ProducerRecord<>(TOPIC,
                "{\"a\":" + time +
                        ",\"b\":" + time + "}")).get();
    }

    public static void main(String[] args) throws Exception {
        Options options = new OptionsBuilder()
                .include(ProducerBenchmark.class.getCanonicalName())
                .forks(1)
                .build();

        new Runner(options).run();
    }
}
