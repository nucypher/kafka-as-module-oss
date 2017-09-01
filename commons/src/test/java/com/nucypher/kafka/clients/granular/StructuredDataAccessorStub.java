package com.nucypher.kafka.clients.granular;

import com.nucypher.kafka.utils.StringUtils;
import org.bouncycastle.util.encoders.Hex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Stub for {@link StructuredDataAccessor} class
 */
public class StructuredDataAccessorStub implements StructuredDataAccessor {

    private List<Line> lines;
    private int index = -1;

    private static class Line {
        private Map<String, String> fields;
        private Map<String, String> encrypted;

        public Line(Map<String, String> fields, Map<String, String> encrypted) {
            this.fields = fields;
            this.encrypted = encrypted;
        }
    }

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
        Map<String, String> encrypted = new HashMap<>();
        if (parts.length > 1) {
            for (String part : parts[1].split(",")) {
                String[] field = part.replaceAll("[\"\\s{}\\[\\]]", "").split(":");
                encrypted.put(field[0], field[1]);
            }
        }
        lines.add(new Line(fields, encrypted));
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
        Line line = lines.get(index);
        Map<String, String> fields = line.fields;
        Map<String, String> encrypted = line.encrypted;
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
            builder.append(", \"encrypted\":{");
            i = 0;
            for (Map.Entry<String, String> field : encrypted.entrySet()) {
                builder.append("\"")
                        .append(field.getKey())
                        .append("\":\"")
                        .append(field.getValue())
                        .append("\"");
                if (++i < encrypted.size()) {
                    builder.append(", ");
                }
            }
            builder.append("}");
        }
        builder.append("}");
        return builder.toString();
    }

    @Override
    public Set<String> getAllFields() {
        Map<String, String> fields = lines.get(0).fields;
        return fields.keySet();
    }

    @Override
    public Map<String, byte[]> getAllEDEKs() {
        Map<String, String> encrypted = lines.get(index).encrypted;
        Map<String, byte[]> result = new HashMap<>();
        for (Map.Entry<String, String> field : encrypted.entrySet()) {
            result.put(field.getKey(), Hex.decode(field.getValue()));
        }
        return result;
    }

    @Override
    public byte[] getEncrypted(String field) {
        return Hex.decode(getUnencrypted(field));
    }

    @Override
    public byte[] getUnencrypted(String field) {
        Map<String, String> fields = lines.get(index).fields;
        return fields.get(field).getBytes();
    }

    @Override
    public void addEncrypted(String field, byte[] data) {
        Map<String, String> fields = lines.get(index).fields;
        fields.put(field, Hex.toHexString(data));
    }

    @Override
    public void addEDEK(String field, byte[] edek) {
        Map<String, String> encrypted = lines.get(index).encrypted;
        encrypted.put(field, Hex.toHexString(edek));
    }

    @Override
    public void addUnencrypted(String field, byte[] data) {
        Map<String, String> fields = lines.get(index).fields;
        fields.put(field, new String(data));
    }

    @Override
    public void removeEDEK(String field) {
        Map<String, String> encrypted = lines.get(index).encrypted;
        if (encrypted.containsKey(field)) {
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
