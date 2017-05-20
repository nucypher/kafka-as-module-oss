package com.nucypher.kafka.clients.granular;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.nucypher.kafka.errors.CommonException;
import com.nucypher.kafka.utils.GranularUtils;
import org.bouncycastle.util.encoders.Hex;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * {@link StructuredDataAccessor} for JSON format
 */
public class JsonDataAccessor extends OneMessageDataAccessor {

    private static final String ENCRYPTED_FIELD = "encrypted";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private JsonNode root;
    private ArrayNode encryptedNode;
    private Map<String, FieldObject> fieldsCache;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public void deserialize(String topic, byte[] data) {
        try {
            root = MAPPER.readTree(data);
        } catch (IOException e) {
            throw new CommonException(e, "Unable to parse JSON data '%s'", new String(data));
        }
        extractEncryptedFields();
        setEmpty(root.size() == 0);
        fieldsCache = new HashMap<>();
    }

    private void extractEncryptedFields() {
        if (!root.isObject()) {
            return;
        }
        CommonException exception = new CommonException(
                "Field '%s' reserved for array of encrypted fields", ENCRYPTED_FIELD);
        JsonNode encrypted = root.get(ENCRYPTED_FIELD);
        if (encrypted == null || encrypted.isNull()) {
            encryptedNode = MAPPER.createArrayNode();
            ObjectNode objectRoot = (ObjectNode) root;
            objectRoot.set(ENCRYPTED_FIELD, encryptedNode);
            return;
        }
        if (!encrypted.isArray()) {
            throw exception;
        }
        encryptedNode = (ArrayNode) encrypted;
        for (JsonNode encryptedField : encryptedNode) {
            if (!encryptedField.isTextual()) {
                throw exception;
            }
        }
    }

    @Override
    public byte[] serialize() {
        if ((encryptedNode == null ||
                encryptedNode.size() == 0) &&
                root.isObject()) {
            ((ObjectNode) root).remove(ENCRYPTED_FIELD);
        }
        return getBytes(root);
    }

    @Override
    public Set<String> getAllFields() {
        return getAll(new HashMap<String, byte[]>(), root, "", false).keySet();
    }

    private Map<String, byte[]> getAll(Map<String, byte[]> fields,
                                       JsonNode node,
                                       String fieldName,
                                       boolean loadData) {
        if (node.isObject()) {
            Iterator<Map.Entry<String, JsonNode>> iterator = node.fields();
            while (iterator.hasNext()) {
                Map.Entry<String, JsonNode> entry = iterator.next();
                getAll(fields, fieldName, entry.getKey(), entry.getValue(), loadData);
            }
        } else {
            int i = 0;
            for (JsonNode child : node) {
                getAll(fields, fieldName, String.valueOf(i + 1), child, loadData);
                i++;
            }
        }

        return fields;
    }

    private void getAll(Map<String, byte[]> fields,
                        String parentFieldName,
                        String childName,
                        JsonNode node,
                        boolean loadData) {
        String fieldName = GranularUtils.getFieldName(parentFieldName, childName);
        if (node.isObject() || node.isArray()) {
            getAll(fields, node, fieldName, loadData);
        } else if (loadData) {
            fields.put(fieldName, getBytes(node));
        } else {
            fields.put(fieldName, null);
        }
    }

    @Override
    public Map<String, byte[]> getAllEncrypted() {
        Map<String, byte[]> fields = new HashMap<>();
        if (root.isObject()) {
            for (JsonNode node : encryptedNode) {
                String field = node.asText();
                fields.put(field, getBytesWithoutQuotes(getUnencrypted(field)));
            }
        } else {
            Map<String, byte[]> fieldsData =
                    getAll(new HashMap<String, byte[]>(), root, "", true);
            for (Map.Entry<String, byte[]> entry : fieldsData.entrySet()) {
                fields.put(entry.getKey(), getBytesWithoutQuotes(entry.getValue()));
            }
        }
        return fields;
    }

    private byte[] getBytesWithoutQuotes(byte[] input) {
        byte[] bytes = Arrays.copyOfRange(input, 1, input.length - 1);
        return Hex.decode(bytes);
    }


    @Override
    public byte[] getUnencrypted(String field) {
        return getBytes(getFieldObject(field).getValue());
    }

    private static byte[] getBytes(JsonNode node) {
        try {
            return MAPPER.writeValueAsBytes(node);
        } catch (JsonProcessingException e) {
            throw new CommonException(e);
        }
    }

    private FieldObject getFieldObject(String path) {
        List<String> pathList = GranularUtils.parsePath(path);

        if (pathList == null || pathList.isEmpty()) {
            throw new CommonException("Input path is empty");
        }
        if (fieldsCache.containsKey(path)) {
            return fieldsCache.get(path);
        }

        JsonNode parentNode = root;
        JsonNode currentNode = root;
        Integer index = null;
        String fieldName = null;
        for (int i = 0; i < pathList.size(); i++) {
            parentNode = currentNode;
            String currentPath = GranularUtils.getFieldName(pathList.subList(0, i + 1));
            if (parentNode.isObject()) {
                fieldName = pathList.get(i);
                currentNode = parentNode.get(fieldName);
                index = null;
            } else if (parentNode.isArray()) {
                index = Integer.valueOf(pathList.get(i)) - 1; //TODO add checking
                currentNode = parentNode.get(index);
                fieldName = null;
            } else {
                throw new CommonException("Field '%s' is neither object nor array", currentPath);
            }
            if (currentNode == null) {
                throw new CommonException("Field '%s' not found", currentPath);
            }
        }

        FieldObject object = new FieldObject(parentNode, currentNode, index, fieldName);
        fieldsCache.put(path, object);
        return object;
    }

    @Override
    public void addEncrypted(String field, byte[] data) {
        JsonNode dataNode = TextNode.valueOf(Hex.toHexString(data));
        getFieldObject(field).setValue(dataNode);

        boolean contains = false;
        for (JsonNode node : encryptedNode) {
            if (node.asText().equals(field)) {
                contains = true;
                break;
            }
        }
        if (!contains) {
            encryptedNode.add(field);
        }
    }

    @Override
    public void addUnencrypted(String field, byte[] data) {
        JsonNode dataNode;
        try {
            dataNode = MAPPER.readTree(data);
        } catch (IOException e) {
            throw new CommonException(e, "Unable to parse JSON data '%s'", new String(data));
        }
        getFieldObject(field).setValue(dataNode);

        int i = 0;
        for (JsonNode node : encryptedNode) {
            if (node.asText().equals(field)) {
                break;
            }
            i++;
        }
        encryptedNode.remove(i);
    }

    private static class FieldObject {
        private JsonNode parentNode;
        private JsonNode childNode;
        private Integer index;
        private String fieldName;

        public FieldObject(JsonNode parentNode,
                           JsonNode childNode,
                           Integer index,
                           String fieldName) {
            this.parentNode = parentNode;
            this.childNode = childNode;
            this.index = index;
            this.fieldName = fieldName;
        }

        public JsonNode getValue() {
            return childNode;
        }

        public void setValue(JsonNode dataNode) {
            if (parentNode.isObject()) {
                ((ObjectNode) parentNode).set(fieldName, dataNode);
            } else {
                ArrayNode arrayNode = (ArrayNode) parentNode;
                arrayNode.remove(index);
                arrayNode.insert(index, dataNode);
            }
        }
    }
}
