package com.nucypher.kafka.clients.granular;

import com.nucypher.kafka.clients.MessageHandler;
import com.nucypher.kafka.utils.WrapperReEncryptionKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Structured message handler for encryption by fields
 *
 * @author szotov
 */
public class StructuredMessageHandler implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(StructuredMessageHandler.class);
    private static final String SEPARATOR = "-";

    private MessageHandler messageHandler;
    //TODO store EDEKs once before message
    private Map<String, byte[]> encryptedFields;
    private StructuredDataAccessor dataAccessor;

    /**
     * Constructor using {@link MessageHandler}
     *
     * @param messageHandler message handler
     */
    public StructuredMessageHandler(MessageHandler messageHandler) {
        this.messageHandler = messageHandler;
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
        dataAccessor.deserialize(topic, data);
        while (dataAccessor.hasNext()) {
            dataAccessor.seekToNext();
            for (String field : fields) {
                byte[] fieldData = dataAccessor.getUnencrypted(field);
                fieldData = messageHandler.encrypt(
                        getFieldName(topic, field), fieldData);
                dataAccessor.addEncrypted(field, fieldData);
            }
        }
        return dataAccessor.serialize();
    }

    private static String getFieldName(String topic, String field) {
        return topic + SEPARATOR + field;
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
        dataAccessor.deserialize(topic, payload);
        while (dataAccessor.hasNext()) {
            dataAccessor.seekToNext();
            for (Map.Entry<String, byte[]> entry : dataAccessor.getAllEncrypted().entrySet()) {
                String field = entry.getKey();
                byte[] fieldData = entry.getValue();
                try {
                    fieldData = messageHandler.decrypt(fieldData);
                    dataAccessor.addUnencrypted(field, fieldData);
                } catch (Exception e) {
                    LOGGER.warn("Field '{}' is not available to decrypt: {}", field, e.getMessage());
                }
            }
        }
        return dataAccessor.serialize();
    }

    /**
     * Get all encrypted fields
     *
     * @param topic        topic
     * @param payload      encrypted data
     * @param dataAccessor data accessor
     * @return all encrypted fields
     */
    public Set<String> getAllEncrypted(String topic,
                                       byte[] payload,
                                       StructuredDataAccessor dataAccessor) {
        this.dataAccessor = dataAccessor;
        dataAccessor.deserialize(topic, payload);
        if (!dataAccessor.hasNext()) {
            encryptedFields = new HashMap<>();
            return Collections.emptySet();
        }
        dataAccessor.seekToNext();
        encryptedFields = dataAccessor.getAllEncrypted();
//        Set<String> fields = dataAccessor.getAllEncrypted().keySet();
//        dataAccessor.reset();
        return encryptedFields.keySet();
    }

    /**
     * Re-encrypt EDEKs and serialize into byte array
     *
     * @param reKeys fields and re-encryption keys
     * @return byte array
     */
    //TODO refactor
    public byte[] reEncrypt(Map<String, WrapperReEncryptionKey> reKeys) {
//        dataAccessor.deserialize(topic, payload);
        //first message is always deserialized
        Map<String, byte[]> fields = encryptedFields;
        reEncrypt(reKeys, fields);

        while (dataAccessor.hasNext()) {
            dataAccessor.seekToNext();
            fields = dataAccessor.getAllEncrypted();
            reEncrypt(reKeys, fields);
        }
        return dataAccessor.serialize();
    }

    private void reEncrypt(Map<String, WrapperReEncryptionKey> reKeys,
                           Map<String, byte[]> fields) {
        for (Map.Entry<String, WrapperReEncryptionKey> entry : reKeys.entrySet()) {
            String field = entry.getKey();
            WrapperReEncryptionKey reKey = entry.getValue();
            byte[] fieldData = fields.get(field);
            fieldData = messageHandler.reEncrypt(fieldData, reKey);
            dataAccessor.addEncrypted(field, fieldData);
        }
    }
}
