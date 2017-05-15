package com.nucypher.kafka.clients.granular;

import com.nucypher.kafka.Pair;
import com.nucypher.kafka.clients.BytesMessage;
import com.nucypher.kafka.clients.ExtraParameters;
import com.nucypher.kafka.clients.Header;
import com.nucypher.kafka.decrypt.ByteDecryptor;
import com.nucypher.kafka.encrypt.ByteEncryptor;
import com.nucypher.kafka.errors.CommonException;
import com.nucypher.kafka.utils.MessageUtils;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.Serializable;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.spec.InvalidKeySpecException;
import java.util.HashMap;
import java.util.Map;

/**
 * Structured message for encryption by fields
 *
 * @author szotov
 */
@Slf4j
@ToString
public class StructuredMessage implements Serializable {

    private String topic;
    private transient Map<String, Pair<ByteEncryptor, ExtraParameters>> encryptors;
    private transient ByteDecryptor decryptor;
    private transient StructuredDataAccessor dataAccessor;
    private transient Map<String, Header> headers = new HashMap<>();
    //used only to prevent double deserialization
    private transient Map<String, BytesMessage> encryptedFieldsFirstMessage = new HashMap<>();
    //TODO add separate storing header and payload to data accessor

    /**
     * Constructor for re-encryption
     *
     * @param messageBytes input message bytes
     * @param dataAccessor data accessor
     */
    public StructuredMessage(byte[] messageBytes,
                             String topic,
                             StructuredDataAccessor dataAccessor) {
        this.topic = topic;
        this.dataAccessor = dataAccessor;
        dataAccessor.deserialize(topic, messageBytes);
    }

    /**
     * Constructor for encryption
     *
     * @param messageBytes input message bytes
     * @param topic        topic name
     * @param dataAccessor data accessor
     * @param encryptors   encryptors map
     */
    public StructuredMessage(byte[] messageBytes,
                             String topic,
                             StructuredDataAccessor dataAccessor,
                             Map<String, Pair<ByteEncryptor, ExtraParameters>> encryptors) {
        this(messageBytes, topic, dataAccessor);
        this.encryptors = encryptors;
    }

    /**
     * Constructor for decryption
     *
     * @param messageBytes input message bytes
     * @param topic        topic name
     * @param dataAccessor data accessor
     * @param decryptor    decryptor
     */
    public StructuredMessage(byte[] messageBytes,
                             String topic,
                             StructuredDataAccessor dataAccessor,
                             ByteDecryptor decryptor) {
        this(messageBytes, topic, dataAccessor);
        this.decryptor = decryptor;
    }

    /**
     * Encrypt message by fields
     *
     * @return encrypted data
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public byte[] encrypt() throws IOException, ClassNotFoundException,
            IllegalAccessException, InstantiationException {
        if (encryptors == null) {
            throw new CommonException("Encryptors are not specified for message encryption");
        }
        while (dataAccessor.hasNext()) {
            dataAccessor.seekToNext();
            encryptOneMessage();
        }
        return dataAccessor.serialize();
    }

    private void encryptOneMessage() throws IOException, ClassNotFoundException {
        for (String field : encryptors.keySet()) {
            byte[] fieldData = dataAccessor.getUnencrypted(field);
            Pair<ByteEncryptor, ExtraParameters> encryptor = encryptors.get(field);
            if (encryptor == null) {
                throw new CommonException("Not found encryptor for field '%s'", field);
            }
            BytesMessage message = new BytesMessage(
                    encryptor.getFirst(),
                    MessageUtils.getHeader(topic, encryptor.getLast()),
                    fieldData);
            dataAccessor.addEncrypted(field, message.encrypt());
        }
    }

    /**
     * Decrypt message by fields
     *
     * @return decrypted data
     * @throws ClassNotFoundException
     * @throws NoSuchProviderException
     * @throws NoSuchAlgorithmException
     * @throws InvalidKeyException
     * @throws IOException
     * @throws InvalidKeySpecException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public byte[] decrypt() throws ClassNotFoundException, NoSuchProviderException,
            NoSuchAlgorithmException, InvalidKeyException, IOException,
            InvalidKeySpecException, IllegalAccessException, InstantiationException {
        if (decryptor == null) {
            throw new CommonException("Decryptor is not specified for message decryption");
        }
        while (dataAccessor.hasNext()) {
            dataAccessor.seekToNext();
            decryptOneMessage();
        }
        return dataAccessor.serialize();
    }

    private void decryptOneMessage() throws IOException, ClassNotFoundException {
        for (Map.Entry<String, byte[]> entry : dataAccessor.getAllEncrypted().entrySet()) {
            String field = entry.getKey();
            byte[] fieldData = entry.getValue();
            BytesMessage fieldMessage = new BytesMessage(decryptor, fieldData);
            try {
                fieldData = fieldMessage.decrypt();
                dataAccessor.addUnencrypted(field, fieldData);
            } catch (Exception e) {
                log.warn("Field '{}' is not available to decrypt: {}", field, e.getMessage());
            }
        }
    }

    /**
     * @return all headers with fields
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public Map<String, Header> getAllHeaders() //TODO refactor
            throws IllegalAccessException, InstantiationException, IOException, ClassNotFoundException {
        if (!dataAccessor.hasNext()) {
            return headers;
        }
        dataAccessor.seekToNext();
        for (Map.Entry<String, byte[]> entry : dataAccessor.getAllEncrypted().entrySet()) {
            String field = entry.getKey();
            BytesMessage fieldMessage = new BytesMessage(entry.getValue());
            //used only to prevent double deserialization
            encryptedFieldsFirstMessage.put(field, fieldMessage);
            headers.put(field, fieldMessage.getHeader());
        }
        dataAccessor.reset();
        return headers;
    }

    /**
     * Add re-encrypted header
     *
     * @param field  field
     * @param header header
     * @throws IOException
     */
    public void setHeader(String field, Header header) throws IOException {
        headers.put(field, header);
    }

    /**
     * Serialize into byte array after EDEKs re-encryption
     *
     * @return byte array
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public byte[] reEncrypt() throws IOException, ClassNotFoundException {
        if (headers.isEmpty()) {
            return dataAccessor.serialize();
        }
        //first message is always deserialized
        dataAccessor.seekToNext();
        for (Map.Entry<String, BytesMessage> entry : encryptedFieldsFirstMessage.entrySet()) {
            String field = entry.getKey();
            BytesMessage fieldMessage = entry.getValue();
            fieldMessage.setHeader(headers.get(field));
            dataAccessor.addEncrypted(field, fieldMessage.serialize());
        }
        while (dataAccessor.hasNext()) {
            dataAccessor.seekToNext();
            for (Map.Entry<String, byte[]> entry : dataAccessor.getAllEncrypted().entrySet()) {
                String field = entry.getKey();
                BytesMessage fieldMessage = new BytesMessage(entry.getValue());
                fieldMessage.setHeader(headers.get(field));
                dataAccessor.addEncrypted(field, fieldMessage.serialize());
            }
        }
        return dataAccessor.serialize();
    }
}
