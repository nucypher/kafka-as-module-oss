package com.nucypher.kafka.clients.example.granular;

import com.nucypher.kafka.clients.example.utils.JaasUtils;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryMain;

import java.io.File;
import java.io.IOException;

/**
 * {@link SchemaRegistryMain} runner
 */
public class SchemaRegistry {

    public static void main(String[] args) throws IOException {
        JaasUtils.initializeConfiguration();
        System.setProperty("zookeeper.sasl.client", "false");

        File file = new File(SchemaRegistry.class.getClassLoader()
                        .getResource("schema-registry.properties").getFile());
        SchemaRegistryMain.main(new String[]{file.getAbsolutePath()});
    }

}
