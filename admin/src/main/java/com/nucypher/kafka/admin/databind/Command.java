package com.nucypher.kafka.admin.databind;

import com.nucypher.crypto.EncryptionAlgorithm;
import com.nucypher.crypto.impl.ElGamalEncryptionAlgorithm;
import com.nucypher.kafka.clients.granular.DataFormat;
import com.nucypher.kafka.utils.KeyType;
import com.nucypher.kafka.zk.ClientType;
import com.nucypher.kafka.zk.EncryptionType;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Set;

/**
 * Command object
 */
public class Command {

    private CommandType commandType;
    private ClientType clientType;
    private String clientKey;
    private String masterKey;
    private EncryptionAlgorithm encryptionAlgorithm = new ElGamalEncryptionAlgorithm();
    private String curveName;
    private String clientName;
    private String channelName;
    private EncryptionType channelType = EncryptionType.FULL;
    private Integer expiredDays;
    private ZonedDateTime expiredDate;
    private String privateKeyPath;
    private String publicKeyPath;
    private KeyType keyType;
    private Set<String> fields;
    private String channelDataAccessor;
    private DataFormat channelDataFormat;

    /**
     * @return command type
     */
    public CommandType getCommandType() {
        return commandType;
    }

    /**
     * @param commandType command type
     */
    public void setCommandType(CommandType commandType) {
        this.commandType = commandType;
    }

    /**
     * @return client type
     */
    public ClientType getClientType() {
        return clientType;
    }

    /**
     * @param clientType client type
     */
    public void setClientType(ClientType clientType) {
        this.clientType = clientType;
    }

    /**
     * @return path to the client private or public (only for consumer) key file
     */
    public String getClientKey() {
        return clientKey;
    }

    /**
     * @param clientKey path to the client private or public (only for consumer) key file
     */
    public void setClientKey(String clientKey) {
        this.clientKey = clientKey;
    }

    /**
     * @return path to the master private key file
     */
    public String getMasterKey() {
        return masterKey;
    }

    /**
     * @param masterKey path to the master private key file
     */
    public void setMasterKey(String masterKey) {
        this.masterKey = masterKey;
    }

    /**
     * @return encryption algorithm
     */
    public EncryptionAlgorithm getEncryptionAlgorithm() {
        return encryptionAlgorithm;
    }

    /**
     * @param encryptionAlgorithm encryption algorithm
     */
    public void setEncryptionAlgorithm(EncryptionAlgorithm encryptionAlgorithm) {
        this.encryptionAlgorithm = encryptionAlgorithm;
    }

    /**
     * @return elliptic curve name
     */
    public String getCurveName() {
        return curveName;
    }

    /**
     * @param curveName elliptic curve name
     */
    public void setCurveName(String curveName) {
        this.curveName = curveName;
    }

    /**
     * @return client principal name
     */
    public String getClientName() {
        return clientName;
    }

    /**
     * @param clientName client principal name
     */
    public void setClientName(String clientName) {
        this.clientName = clientName;
    }

    /**
     * @return channel which is used for the key.
     * If not set then key is using for all channels
     */
    public String getChannelName() {
        return channelName;
    }

    /**
     * @param channelName channel which is used for the key.
     *                    If not set then key is using for all channels
     */
    public void setChannelName(String channelName) {
        this.channelName = channelName;
    }

    /**
     * @return channel encryption type
     */
    public EncryptionType getChannelType() {
        return channelType;
    }

    /**
     * @param channelType channel encryption type
     */
    public void setChannelType(EncryptionType channelType) {
        this.channelType = channelType;
    }

    /**
     * @return lifetime of the re-encryption key in days
     */
    public Integer getExpiredDays() {
        return expiredDays;
    }

    /**
     * @param expiredDays lifetime of the re-encryption key in days
     */
    public void setExpiredDays(Integer expiredDays) {
        this.expiredDays = expiredDays;
    }

    /**
     * @return expiredDate date of the re-encryption key in ISO-8601 format
     */
    public ZonedDateTime getExpiredDate() {
        return expiredDate;
    }

    /**
     * @param expiredDate expiredDate date of the re-encryption key in ISO-8601 format
     */
    public void setExpiredDate(ZonedDateTime expiredDate) {
        this.expiredDate = expiredDate;
    }

    /**
     * @return path to the private key file
     */
    public String getPrivateKeyPath() {
        return privateKeyPath;
    }

    /**
     * @param privateKeyPath path to the private key file
     */
    public void setPrivateKeyPath(String privateKeyPath) {
        this.privateKeyPath = privateKeyPath;
    }

    /**
     * @return path to the public key file
     */
    public String getPublicKeyPath() {
        return publicKeyPath;
    }

    /**
     * @param publicKeyPath path to the public key file
     */
    public void setPublicKeyPath(String publicKeyPath) {
        this.publicKeyPath = publicKeyPath;
    }

    /**
     * @return generated or client's key type (only for consumer)
     */
    public KeyType getKeyType() {
        return keyType;
    }

    /**
     * @param keyType generated or client's key type (only for consumer)
     */
    public void setKeyType(KeyType keyType) {
        this.keyType = keyType;
    }

    /**
     * @return the fields in the data structure to which this key belongs.
     * Adding key: may be set multiple values.
     * Deleting key: if value is null and channel type is GRANULAR then will be deleted all subkeys
     */
    public Set<String> getFields() {
        return fields;
    }

    /**
     * @param fields the fields in the data structure to which this key belongs.
     *               Adding key: may be set multiple values.
     *               Deleting key: if value is null and channel type is GRANULAR then will be deleted all subkeys
     */
    public void setFields(Set<String> fields) {
        this.fields = fields;
    }

    /**
     * @return accessor class name
     * (only for channels with granular encryption type or keys with fields)
     */
    public String getChannelDataAccessor() {
        return channelDataAccessor;
    }

    /**
     * @param channelDataAccessor accessor class name
     *                            (only for channels with granular encryption type or keys with fields)
     */
    public void setChannelDataAccessor(String channelDataAccessor) {
        this.channelDataAccessor = channelDataAccessor;
    }

    /**
     * @return data format
     * (only for channels with granular encryption type  or keys with fields)
     */
    public DataFormat getChannelDataFormat() {
        return channelDataFormat;
    }

    /**
     * @param channelDataFormat data format
     *                          (only for channels with granular encryption type  or keys with fields)
     */
    public void setChannelDataFormat(DataFormat channelDataFormat) {
        this.channelDataFormat = channelDataFormat;
    }
}
