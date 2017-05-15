package com.nucypher.kafka.clients;

import com.nucypher.kafka.utils.AvroUtils;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.avro.util.Utf8;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Header
 */
@Setter
@Getter
@Builder
@ToString
@AllArgsConstructor
public class Header extends SpecificRecordBase implements SpecificRecord, Serializable {

    private static final Schema schema = new Schema.Parser().parse(
            "{\"type\":\"record\"," +
                    "\"name\":\"Header\"," +
                    "\"namespace\":\"com.nucypher.kafka.clients\"," +
                    "\"fields\":[{\"name\":\"topic\",\"type\":[\"string\", \"null\"]}," +
                    "{\"name\":\"description\",\"type\":[\"string\", \"null\"]}," +
                    "{\"name\":\"map\",\"type\":[{\"type\":\"map\",\"values\":\"bytes\"}, \"null\"]}]}");

    private String topic;               // some fields has an explicit declaration some other hide inside map
    private String description;
    private Map<String, byte[]> map;    // extra parameters for header - like IV, EDEK & etc

    public Header() {
        this.map = new HashMap<>();
    }

    @Override
    public Schema getSchema() {
        return schema;
    }

    @Override
    public Object get(int field) {
        switch (field) {
            case 0:
                if (topic == null) {
                    return null;
                }
                return new Utf8(topic);
            case 1:
                if (description == null) {
                    return null;
                }
                return new Utf8(description);
            case 2:
                if (map == null) {
                    return null;
                }
                Map<Object, ByteBuffer> value = new HashMap<>(map.size());
                for (Map.Entry<String, byte[]> entry : map.entrySet()) {
                    if (entry.getValue() != null) {
                        value.put(new Utf8(entry.getKey()), ByteBuffer.wrap(entry.getValue()));
                    }
                }
                return value;
            default:
                throw new AvroRuntimeException("Bad index");
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void put(int field, Object value) {
        switch (field) {
            case 0:
                if (value == null) {
                    topic = null;
                } else {
                    topic = value.toString();
                }
                break;
            case 1:
                if (value == null) {
                    description = null;
                } else {
                    description = value.toString();
                }
                break;
            case 2:
                if (value == null) {
                    map = null;
                } else {
                    Map<Object, ByteBuffer> mapValue = (Map<Object, ByteBuffer>) value;
                    map = new HashMap<>(mapValue.size());
                    for (Map.Entry<Object, ByteBuffer> entry : mapValue.entrySet()) {
                        map.put(entry.getKey().toString(), entry.getValue().array());
                    }
                }
                break;
            default:
                throw new AvroRuntimeException("Bad index");
        }
    }

    public Header(String topic) {
        this();
        this.topic = topic;
    }

    /**
     * Create Header from serialized byte array
     *
     * @param serializedHeader -
     */
    public Header(byte[] serializedHeader) throws IOException, ClassNotFoundException {
        // TODO extra checks
        Header header = deserialize(serializedHeader);
        this.topic = header.getTopic();
        this.description = header.getDescription();
        this.map = header.getMap();
    }

    /**
     * add extra parameters to header
     *
     * @param key   -
     * @param value -
     * @return -
     */
    public Header add(String key, byte[] value) {
        // TODO extra checks for variables
        if (map == null) {
            this.map = new HashMap<>();
        }
        map.put(key, value);
        return this;
    }

    /**
     * Serialize Header into byte array
     *
     * @return - byte[]
     * @throws IOException -
     */
    public byte[] serialize() throws IOException {
        return AvroUtils.serialize(schema, this);
    }

    /**
     * Deserialize bytes into {@link Header} object
     *
     * @param bytes bytes
     * @return {@link Header} object
     */
    public static Header deserialize(byte[] bytes) {
        return (Header) AvroUtils.deserialize(schema, bytes, true);
    }

}
