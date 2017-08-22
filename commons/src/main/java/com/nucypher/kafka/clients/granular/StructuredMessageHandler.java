package com.nucypher.kafka.clients.granular;

import com.nucypher.kafka.clients.EncryptedDataEncryptionKey;
import com.nucypher.kafka.clients.Message;
import com.nucypher.kafka.clients.MessageHandler;
import com.nucypher.kafka.utils.GranularUtils;
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

    private MessageHandler messageHandler;
    //TODO store EDEKs once before message
    private Map<String, byte[]> edeks;
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
                Message message = messageHandler.encryptMessage(
                        GranularUtils.getChannelFieldName(topic, field), fieldData);
                EncryptedDataEncryptionKey edek = message.getEDEK();
                message.setEDEK(null);
                dataAccessor.addEDEK(field, edek.serialize());
                dataAccessor.addEncrypted(field, message.serialize());
            }
        }
        return dataAccessor.serialize();
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
            for (Map.Entry<String, byte[]> entry : dataAccessor.getAllEDEKs().entrySet()) {
                String field = entry.getKey();
                EncryptedDataEncryptionKey edek = EncryptedDataEncryptionKey.deserialize(
                        entry.getValue());
                byte[] fieldData = dataAccessor.getEncrypted(field);
                Message message = Message.deserialize(fieldData);
                message.setEDEK(edek);
                try {
                    fieldData = messageHandler.decryptMessage(message);
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
            edeks = new HashMap<>();
            return Collections.emptySet();
        }
        dataAccessor.seekToNext();
        edeks = dataAccessor.getAllEDEKs();
        return edeks.keySet();
    }

    /**
     * Re-encrypt EDEKs and serialize into byte array
     *
     * @param topic        topic
     * @param reKeys fields and re-encryption keys
     * @return byte array
     */
    //TODO refactor
    public byte[] reEncrypt(String topic,
                            Map<String, WrapperReEncryptionKey> reKeys) {
        //first message is always deserialized
        Map<String, byte[]> edeks = this.edeks;
        reEncrypt(topic, reKeys, edeks);

        while (dataAccessor.hasNext()) {
            dataAccessor.seekToNext();
            edeks = dataAccessor.getAllEDEKs();
            reEncrypt(topic, reKeys, edeks);
        }
        return dataAccessor.serialize();
    }

    private void reEncrypt(String topic,
                           Map<String, WrapperReEncryptionKey> reKeys,
                           Map<String, byte[]> edeks) {
        for (Map.Entry<String, WrapperReEncryptionKey> entry : reKeys.entrySet()) {
            String field = entry.getKey();
            WrapperReEncryptionKey reKey = entry.getValue();
            byte[] bytes = edeks.get(field);
            EncryptedDataEncryptionKey edek = EncryptedDataEncryptionKey.deserialize(bytes);
            edek = messageHandler.reEncryptEDEK(
                    GranularUtils.getChannelFieldName(topic, field), edek, reKey);
            dataAccessor.addEDEK(field, edek.serialize());
        }
    }
}
