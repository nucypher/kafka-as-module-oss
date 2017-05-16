package com.nucypher.kafka.clients.granular;

import com.nucypher.kafka.Pair;
import com.nucypher.kafka.utils.StringUtils;
import org.bouncycastle.util.encoders.Hex;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Stub for {@link StructuredDataAccessor} class
 */
public class StructuredDataAccessorStub implements StructuredDataAccessor {

    private List<Pair<Map<String, String>, List<String>>> lines;
    private int index = -1;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public void deserialize(String topic, byte[] data) {
        lines = new ArrayList<>();
        String[] parts = new String(data).split("\n");
        for (String part : parts) {
            deserialize(part);
        }
        reset();
    }

    private void deserialize(String line) {
        Map<String, String> fields = new HashMap<>();
        String[] parts = line.split("\"encrypted\":");
        for (String part : parts[0].split(",")) {
            String[] field = part.replaceAll("[\"\\s{}\\[\\]]", "").split(":");
            if (StringUtils.isNotBlank(field[0])) {
                fields.put(field[0], field[1]);
            }
        }
        List<String> encrypted = new ArrayList<>();
        if (parts.length > 1) {
            String field = parts[1].replaceAll("[\"\\s{}\\[\\]]", "");
            Collections.addAll(encrypted, field.split(","));
        }
        lines.add(new Pair<>(fields, encrypted));
    }

    @Override
    public byte[] serialize() {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < lines.size(); i++) {
            builder.append(serialize(i));
            if (i < lines.size() - 1) {
                builder.append("\n");
            }
        }
        return builder.toString().getBytes();
    }

    private String serialize(int index) {
        Pair<Map<String, String>, List<String>> line = lines.get(index);
        Map<String, String> fields = line.getFirst();
        List<String> encrypted = line.getLast();
        StringBuilder builder = new StringBuilder("{");
        int i = 0;
        for (Map.Entry<String, String> entry : fields.entrySet()) {
            String field = entry.getKey();
            String value = entry.getValue();
            builder.append("\"")
                    .append(field)
                    .append("\":\"")
                    .append(value)
                    .append("\"");
            if (++i < fields.entrySet().size()) {
                builder.append(", ");
            }
        }
        if (encrypted != null && !encrypted.isEmpty()) {
            builder.append(", \"encrypted\":[");
            i = 0;
            for (String field : encrypted) {
                builder.append("\"")
                        .append(field)
                        .append("\"");
                if (++i < encrypted.size()) {
                    builder.append(", ");
                }
            }
            builder.append("]");
        }
        builder.append("}");
        return builder.toString();
    }

    @Override
    public Set<String> getAllFields() {
        Map<String, String> fields = lines.get(0).getFirst();
        return fields.keySet();
    }

    @Override
    public Map<String, byte[]> getAllEncrypted() {
        List<String> encrypted = lines.get(index).getLast();
        Map<String, byte[]> result = new HashMap<>();
        for (String field : encrypted) {
            result.put(field, Hex.decode(getUnencrypted(field)));
        }
        return result;
    }

    @Override
    public byte[] getUnencrypted(String field) {
        Map<String, String> fields = lines.get(index).getFirst();
        return fields.get(field).getBytes();
    }

    @Override
    public void addEncrypted(String field, byte[] data) {
        List<String> encrypted = lines.get(index).getLast();
        if (!encrypted.contains(field)) {
            encrypted.add(field);
        }
        Map<String, String> fields = lines.get(index).getFirst();
        fields.put(field, Hex.toHexString(data));
    }

    @Override
    public void addUnencrypted(String field, byte[] data) {
        Map<String, String> fields = lines.get(index).getFirst();
        List<String> encrypted = lines.get(index).getLast();
        fields.put(field, new String(data));
        if (encrypted.contains(field)) {
            encrypted.remove(field);
        }
    }

    @Override
    public boolean hasNext() {
        return index < lines.size() - 1;
    }

    @Override
    public void seekToNext() throws NoSuchElementException {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        index++;
    }

    @Override
    public void reset() {
        index = -1;
    }
}
