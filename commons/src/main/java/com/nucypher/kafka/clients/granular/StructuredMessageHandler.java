package com.nucypher.kafka.clients.granular;

import com.google.common.primitives.Ints;
import com.nucypher.kafka.clients.EncryptedDataEncryptionKey;
import com.nucypher.kafka.clients.Message;
import com.nucypher.kafka.clients.MessageHandler;
import com.nucypher.kafka.encrypt.DataEncryptionKeyManager;
import com.nucypher.kafka.errors.CommonException;
import com.nucypher.kafka.utils.GranularUtils;
import com.nucypher.kafka.utils.WrapperReEncryptionKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.security.Key;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Structured message handler for encryption by fields
 *
 * @author szotov
 */
public class StructuredMessageHandler implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(StructuredMessageHandler.class);

    private MessageHandler messageHandler;
    private DataEncryptionKeyManager keyManager;

    private InnerMessage innerMessage;

    /**
     * Constructor using {@link MessageHandler}
     *
     * @param messageHandler message handler
     */
    public StructuredMessageHandler(MessageHandler messageHandler) {
        this.messageHandler = messageHandler;
        keyManager = messageHandler.getDataEncryptionKeyManager();
    }

    /**
     * Encrypt byte array
     *
     * @param topic        topic
     * @param data         data
     * @param dataAccessor data accessor
     * @param fields       fields to encrypt
     * @return encrypted data
     */
    public byte[] encrypt(String topic,
                          byte[] data,
                          StructuredDataAccessor dataAccessor,
                          Set<String> fields) {
        Map<String, Key> deks = generateDEKs(topic, fields);
        Map<String, byte[]> edeks = encryptDEKs(topic, deks);

        boolean firstData = true;
        dataAccessor.deserialize(topic, data);
        while (dataAccessor.hasNext()) {
            dataAccessor.seekToNext();
            if (firstData) {
                updateEDEKs(dataAccessor, edeks);
                firstData = false;
            }
            for (Map.Entry<String, Key> entry : deks.entrySet()) {
                String field = entry.getKey();
                Key dek = entry.getValue();
                byte[] fieldData = dataAccessor.getUnencrypted(field);
                Message message = messageHandler.encryptMessage(fieldData, dek);
                dataAccessor.addEncrypted(field, message.serialize());
            }
        }
        return serializeMessage(edeks, dataAccessor.serialize());
    }

    private Map<String, Key> generateDEKs(String topic, Set<String> fields) {
        Map<String, Key> deks = new HashMap<>(fields.size());
        for (String field : fields) {
            Key dek = keyManager.getDEK(
                    GranularUtils.getChannelFieldName(topic, field));
            deks.put(field, dek);
        }
        return deks;
    }

    private Map<String, byte[]> encryptDEKs(String topic, Map<String, Key> deks) {
        Map<String, byte[]> edeks = new HashMap<>(deks.size());
        for (Map.Entry<String, Key> entry : deks.entrySet()) {
            String field = entry.getKey();
            Key dek = entry.getValue();
            byte[] edek = keyManager.encryptDEK(dek,
                    GranularUtils.getChannelFieldName(topic, field));
            edeks.put(field, new EncryptedDataEncryptionKey(edek).serialize());
        }
        return edeks;
    }

    private void updateEDEKs(StructuredDataAccessor dataAccessor,
                             Map<String, byte[]> edeks) {
        Map<String, byte[]> edeksFromMessage = dataAccessor.getAllEDEKs();
        if (!edeksFromMessage.isEmpty()) {
            for (String edekField : edeksFromMessage.keySet()) {
                if (edeks.containsKey(edekField)) {
                    throw new CommonException("Field [%s] already encrypted", edekField);
                }
                dataAccessor.removeEDEK(edekField);
            }
            edeks.putAll(edeksFromMessage);
        }
    }

    /**
     * Decrypt byte array
     *
     * @param topic        topic
     * @param payload      encrypted data
     * @param dataAccessor data accessor
     * @return decrypted data
     */
    public byte[] decrypt(String topic,
                          byte[] payload,
                          StructuredDataAccessor dataAccessor) {
        InnerMessage innerMessage = deserializeMessage(payload);
        Map<String, byte[]> edeks = innerMessage.edeks;
        byte[] data = innerMessage.data;
        Map<String, Key> deks = decryptEDEKs(edeks);

        boolean firstData = true;
        dataAccessor.deserialize(topic, data);
        //TODO refactor
        while (dataAccessor.hasNext()) {
            dataAccessor.seekToNext();
            Iterator<Map.Entry<String, Key>> iterator = deks.entrySet().iterator();
            while(iterator.hasNext()) {
                Map.Entry<String, Key> entry = iterator.next();
                String field = entry.getKey();
                Key dek = entry.getValue();
                byte[] fieldData = dataAccessor.getEncrypted(field);
                Message message = Message.deserialize(fieldData);
                try {
                    fieldData = messageHandler.decryptMessage(message, dek);
                    dataAccessor.addUnencrypted(field, fieldData);
                } catch (Exception e) {
                    if (firstData) {
                        LOGGER.warn("Field '{}' is not available to decrypt: {}",
                                field, e.getMessage());
                        iterator.remove();
                    } else {
                        throw e;
                    }
                }
            }
            if (firstData) {
                addEDEKs(edeks, deks.keySet(), dataAccessor);
                firstData = false;
            }
        }
        return dataAccessor.serialize();
    }

    private Map<String, Key> decryptEDEKs(Map<String, byte[]> edeks) {
        Map<String, Key> deks = new HashMap<>(edeks.size());
        for (Map.Entry<String, byte[]> entry : edeks.entrySet()) {
            String field = entry.getKey();
            EncryptedDataEncryptionKey edek = EncryptedDataEncryptionKey.deserialize(
                    entry.getValue());
            try {
                Key dek = keyManager.decryptEDEK(edek.getBytes(), edek.isComplex());
                deks.put(field, dek);
            } catch (Exception e) {
                LOGGER.warn("Field '{}' is not available to decrypt: {}", field, e.getMessage());
            }
        }
        return deks;
    }

    private void addEDEKs(Map<String, byte[]> edeks,
                          Set<String> decryptedFields,
                          StructuredDataAccessor dataAccessor) {
        if (edeks.size() > decryptedFields.size()) {
            for (Map.Entry<String, byte[]> entry : edeks.entrySet()) {
                if (!decryptedFields.contains(entry.getKey())) {
                    dataAccessor.addEDEK(entry.getKey(), entry.getValue());
                }
            }
        }
    }

    /**
     * Get all encrypted fields
     *
     * @param payload encrypted data
     * @return all encrypted fields
     */
    //TODO refactor
    public Set<String> getAllEncryptedFields(byte[] payload) {
        innerMessage = deserializeMessage(payload);
        return innerMessage.edeks.keySet();
    }

    /**
     * Re-encrypt EDEKs and serialize into byte array
     *
     * @param topic  topic
     * @param reKeys fields and re-encryption keys
     * @return byte array
     */
    public byte[] reEncrypt(String topic,
                            Map<String, WrapperReEncryptionKey> reKeys) {
        Map<String, byte[]> edeks = innerMessage.edeks;
        for (Map.Entry<String, WrapperReEncryptionKey> entry : reKeys.entrySet()) {
            String field = entry.getKey();
            WrapperReEncryptionKey reKey = entry.getValue();
            byte[] bytes = edeks.get(field);
            EncryptedDataEncryptionKey edek = EncryptedDataEncryptionKey.deserialize(bytes);
            edek = messageHandler.reEncryptEDEK(
                    GranularUtils.getChannelFieldName(topic, field), edek, reKey);
            edeks.put(field, edek.serialize());
        }

        return serializeMessage(edeks, innerMessage.data);
    }

    /**
     * Deserialize inner message
     *
     * @param payload payload
     * @return EDEKs and data
     */
    static InnerMessage deserializeMessage(byte[] payload) {
        byte[] data = new byte[
                Ints.fromBytes(payload[0], payload[1], payload[2], payload[3])];
        System.arraycopy(payload, 4, data, 0, data.length);
        int i = Ints.BYTES + data.length;
        Map<String, byte[]> edeks = new HashMap<>();
        while (i < payload.length) {
            byte[] field = new byte[
                    Ints.fromBytes(payload[i], payload[i + 1], payload[i + 2], payload[i + 3])];
            System.arraycopy(payload, i + 4, field, 0, field.length);
            i += 4 + field.length;
            byte[] edek = new byte[
                    Ints.fromBytes(payload[i], payload[i + 1], payload[i + 2], payload[i + 3])];
            System.arraycopy(payload, i + 4, edek, 0, edek.length);
            i += 4 + edek.length;
            edeks.put(new String(field), edek);
        }
        return new InnerMessage(edeks, data);
    }

    /**
     * Serialize EDEKs and data into inner message
     *
     * @param edeks EDEKs
     * @param data  data
     * @return payload
     */
    static byte[] serializeMessage(Map<String, byte[]> edeks, byte[] data) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        byte[] dataLength = Ints.toByteArray(data.length);
        try {
            outputStream.write(dataLength);
            outputStream.write(data);
            for (Map.Entry<String, byte[]> entry : edeks.entrySet()) {
                byte[] field = entry.getKey().getBytes();
                byte[] edek = entry.getValue();
                byte[] fieldLength = Ints.toByteArray(field.length);
                byte[] edekLength = Ints.toByteArray(edek.length);
                outputStream.write(fieldLength);
                outputStream.write(field);
                //TODO optimize EDEK serialization
                outputStream.write(edekLength);
                outputStream.write(edek);
            }
        } catch (IOException e) {
            throw new CommonException(e);
        }
        return outputStream.toByteArray();
    }

    /**
     * Inner message
     */
    static class InnerMessage {
        private Map<String, byte[]> edeks;
        private byte[] data;

        public InnerMessage(Map<String, byte[]> edeks, byte[] data) {
            this.edeks = edeks;
            this.data = data;
        }
    }
}
