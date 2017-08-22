package com.nucypher.kafka.clients.granular;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.nucypher.kafka.errors.CommonException;
import com.nucypher.kafka.utils.GranularUtils;

import javax.xml.bind.DatatypeConverter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * {@link StructuredDataAccessor} for JSON format
 */
public class JsonDataAccessor extends OneMessageDataAccessor {

    private static final String EDEKS_FIELD = "edeks";
    private static final String EDEK_FIELD = "edek";
    private static final String DATA_FIELD = "data";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private JsonNode root;
    private ObjectNode edeksNode;
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
        extractEDEKs();
        setEmpty(root.size() == 0);
        fieldsCache = new HashMap<>();
    }

    private void extractEDEKs() {
        if (!root.isObject()) {
            return;
        }
        JsonNode edeks = root.get(EDEKS_FIELD);
        if (edeks == null || edeks.isNull()) {
            edeksNode = MAPPER.createObjectNode();
            ObjectNode objectRoot = (ObjectNode) root;
            objectRoot.set(EDEKS_FIELD, edeksNode);
            return;
        }
        if (!edeks.isObject()) {
            throw new CommonException(
                    "Field '%s' reserved for map of EDEKs", EDEKS_FIELD);
        }
        edeksNode = (ObjectNode) edeks;
        Iterator<Map.Entry<String, JsonNode>> iterator = edeksNode.fields();
        while (iterator.hasNext()) {
            Map.Entry<String, JsonNode> entry = iterator.next();
            if (!entry.getValue().isTextual()) {
                throw new CommonException(
                        "Field '%s' reserved for map of EDEKs", EDEKS_FIELD);
            }
        }
    }

    @Override
    public byte[] serialize() {
        if ((edeksNode == null ||
                edeksNode.size() == 0) &&
                root.isObject()) {
            ((ObjectNode) root).remove(EDEKS_FIELD);
        }
        return getBytes(root);
    }

    @Override
    public Set<String> getAllFields() {
        return getAll(new HashMap<String, String>(), root, "", false).keySet();
    }

    //TODO refactor
    private Map<String, String> getAll(Map<String, String> fields,
                                       JsonNode node,
                                       String fieldName,
                                       boolean loadData) {
        JsonNode edek = node.get(EDEK_FIELD);
        if (node.isObject() && edek == null) {
            Iterator<Map.Entry<String, JsonNode>> iterator = node.fields();
            while (iterator.hasNext()) {
                Map.Entry<String, JsonNode> entry = iterator.next();
                if (!entry.getKey().equals(EDEKS_FIELD)) {
                    getAll(fields, fieldName, entry.getKey(), entry.getValue(), loadData);
                }
            }
        } else if (node.isObject() && edek != null) {
            fields.put(fieldName, loadData ? edek.asText() : null);
        } else {
            int i = 0;
            for (JsonNode child : node) {
                getAll(fields, fieldName, String.valueOf(i + 1), child, loadData);
                i++;
            }
        }

        return fields;
    }

    private void getAll(Map<String, String> fields,
                        String parentFieldName,
                        String childName,
                        JsonNode node,
                        boolean loadData) {
        String fieldName = GranularUtils.getFieldName(parentFieldName, childName);
        if (node.isObject() || node.isArray()) {
            getAll(fields, node, fieldName, loadData);
        } else if (loadData) {
            fields.put(fieldName, node.asText());
        } else {
            fields.put(fieldName, null);
        }
    }

    @Override
    public Map<String, byte[]> getAllEDEKs() {
        Map<String, byte[]> fields = new HashMap<>();
        if (root.isObject()) {
            Iterator<Map.Entry<String, JsonNode>> iterator = edeksNode.fields();
            while (iterator.hasNext()) {
                Map.Entry<String, JsonNode> entry = iterator.next();
                fields.put(entry.getKey(), getBytes(entry.getValue().asText()));
            }
        } else {
            Map<String, String> fieldsData =
                    getAll(new HashMap<String, String>(), root, "", true);
            for (Map.Entry<String, String> entry : fieldsData.entrySet()) {
                fields.put(entry.getKey(), getBytes(entry.getValue()));
            }
        }
        return fields;
    }

    private byte[] getBytesWithoutQuotes(byte[] input) {
        return getBytes(new String(input, 1, input.length - 1));
    }

    private byte[] getBytes(String input) {
        return DatatypeConverter.parseBase64Binary(input);
    }

    @Override
    public byte[] getEncrypted(String field) {
        if (root.isObject()) {
            return getBytesWithoutQuotes(getUnencrypted(field));
        } else {
            return getBytesWithoutQuotes(getUnencrypted(field + "." + DATA_FIELD));
        }
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
        JsonNode dataNode = TextNode.valueOf(DatatypeConverter.printBase64Binary(data));
        FieldObject fieldObject = getFieldObject(field);
        if (root.isObject()) {
            fieldObject.setValue(dataNode);
        } else {
            ObjectNode objectNode;
            if (!fieldObject.getValue().isObject() || !fieldObject.getValue().has(EDEK_FIELD)) {
                objectNode = MAPPER.createObjectNode();
                objectNode.set(EDEK_FIELD, null);
                fieldObject.setValue(objectNode);
            } else {
                objectNode = (ObjectNode) fieldObject.getValue();
            }
            objectNode.set(DATA_FIELD, dataNode);
        }
    }

    @Override
    public void addEDEK(String field, byte[] data) {
        JsonNode dataNode = TextNode.valueOf(DatatypeConverter.printBase64Binary(data));
        if (root.isObject()) {
            edeksNode.set(field, dataNode);
        } else {
            FieldObject fieldObject = getFieldObject(field);
            ObjectNode objectNode;
            if (!fieldObject.getValue().isObject() || !fieldObject.getValue().has(EDEK_FIELD)) {
                objectNode = MAPPER.createObjectNode();
                objectNode.set(DATA_FIELD, fieldObject.getValue());
                fieldObject.setValue(objectNode);
            } else {
                objectNode = (ObjectNode) fieldObject.getValue();
            }
            objectNode.set(EDEK_FIELD, dataNode);
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

        edeksNode.remove(field);
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
            childNode = dataNode;
        }
    }
}
