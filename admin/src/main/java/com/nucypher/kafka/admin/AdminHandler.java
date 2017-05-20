package com.nucypher.kafka.admin;

import com.nucypher.kafka.DefaultProvider;
import com.nucypher.kafka.clients.granular.DataFormat;
import com.nucypher.kafka.clients.granular.StructuredDataAccessor;
import com.nucypher.kafka.errors.CommonException;
import com.nucypher.kafka.utils.EncryptionAlgorithm;
import com.nucypher.kafka.utils.GranularUtils;
import com.nucypher.kafka.utils.KeyType;
import com.nucypher.kafka.utils.KeyUtils;
import com.nucypher.kafka.utils.WrapperReEncryptionKey;
import com.nucypher.kafka.zk.Channel;
import com.nucypher.kafka.zk.ClientType;
import com.nucypher.kafka.zk.EncryptionType;
import com.nucypher.kafka.zk.KeyHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * Class for handling admin requests
 *
 * @author szotov
 */
public class AdminHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(AdminHandler.class);

    private AdminZooKeeperHandler zooKeeperHandler;

    public AdminHandler(AdminZooKeeperHandler handler) {
        zooKeeperHandler = handler;
    }

    /**
     * Generate re-encryption key and save it to the storage
     *
     * @param algorithm         encryption algorithm
     * @param masterKey         the master key file path
     * @param clientKey         the client key file path
     * @param curve             the name of the curve requested
     * @param clientType        client type
     * @param keyType           client's key type (only for consumer)
     * @param clientName        client principal name
     * @param channel           channel which is used for the key. If null then key is using
     *                          for all channels
     * @param expiredDays       lifetime in days (if expired field is null)
     * @param expired           expired date of the re-encryption key (if expiredDays field is null)
     * @param fields            collection of fields for which re-encryption keys will be generating
     * @param accessorClassName accessor class name (only if fields are not null and format is null)
     * @param format            data format (only if fields are not null and accessorClassName is null
     * @throws CommonException if problems with generating re-encryption key
     */
    public void generateAndSaveReEncryptionKey(
            EncryptionAlgorithm algorithm,
            String masterKey,
            String clientKey,
            String curve,
            ClientType clientType,
            KeyType keyType,
            String clientName,
            String channel,
            Integer expiredDays,
            ZonedDateTime expired,
            Collection<String> fields,
            String accessorClassName,
            DataFormat format)
            throws CommonException {
        BiFunction<String, WrapperReEncryptionKey, KeyHolder> creatorFunction;
        creatorFunction = (String field, WrapperReEncryptionKey key) ->
        {
            KeyHolder.Builder builder = KeyHolder.builder()
                    .setChannel(channel)
                    .setName(clientName)
                    .setType(clientType)
                    .setKey(key)
                    .setField(field);
            if (expired == null) {
                builder.setExpiredDays(expiredDays);
            } else {
                builder.setExpiredDate(expired.toInstant().toEpochMilli());
            }
            return builder.build();
        };

        Class<? extends StructuredDataAccessor> clazz;
        if (format != null) {
            clazz = format.getAccessorClass();
        } else {
            clazz = getAccessorClass(accessorClassName);
        }

        generateAndSaveReEncryptionKey(algorithm, masterKey, clientKey, curve,
                clientType, keyType, channel, fields, clazz, creatorFunction);
    }

    private void generateAndSaveReEncryptionKey(
            EncryptionAlgorithm algorithm,
            String masterKey,
            String clientKey,
            String curve,
            ClientType clientType,
            KeyType keyType,
            String channel,
            Collection<String> fields,
            Class<? extends StructuredDataAccessor> clazz,
            BiFunction<String, WrapperReEncryptionKey, KeyHolder> creatorFunction)
            throws CommonException {
        if (fields != null && fields.size() == 0) {
            fields = null;
        }
        EncryptionType type;

        Map<String, WrapperReEncryptionKey> reKeys = new HashMap<>();
        if (clientKey == null && fields == null) {
            type = EncryptionType.FULL;
            reKeys.put(null, new WrapperReEncryptionKey());
            LOGGER.debug("Empty key was created");
        } else if (clientKey == null) {
            type = EncryptionType.GRANULAR;
            for (String field : fields) {
                reKeys.put(field, new WrapperReEncryptionKey());
            }
            LOGGER.debug("{} empty keys were created", reKeys.size());
        } else if (fields == null) {
            type = EncryptionType.FULL;
            reKeys.put(null, getReEncryptionKey(
                    algorithm, masterKey, clientKey, curve, clientType, keyType));
        } else {
            type = EncryptionType.GRANULAR;
            reKeys = getReEncryptionKey(
                    algorithm, masterKey, clientKey, curve, clientType, keyType, fields);
        }

        if (type != EncryptionType.GRANULAR || clazz != null) {
            zooKeeperHandler.createChannelInZooKeeper(channel, type, clazz);
        }

        for (Map.Entry<String, WrapperReEncryptionKey> entry : reKeys.entrySet()) {
            KeyHolder keyHolder = creatorFunction.apply(entry.getKey(), entry.getValue());
            zooKeeperHandler.saveKeyToZooKeeper(keyHolder);
            LOGGER.info("Key for user '{}' with type '{}' for channel '{}' and field '{}' was created",
                    keyHolder.getName(),
                    keyHolder.getType(),
                    keyHolder.getChannel() == null ? "all" : keyHolder.getChannel(),
                    keyHolder.getField() == null ? "all" : keyHolder.getField());
        }
    }

    @SuppressWarnings("unchecked")
    private static Class<? extends StructuredDataAccessor> getAccessorClass(String accessorClassName) {
        if (accessorClassName == null) {
            return null;
        }
        try {
            return (Class<? extends StructuredDataAccessor>) Class.forName(accessorClassName);
        } catch (ClassNotFoundException e) {
            throw new CommonException(e);
        }
    }

    private Map<String, WrapperReEncryptionKey> getReEncryptionKey(
            EncryptionAlgorithm algorithm,
            String masterKey,
            String clientKey,
            String curve,
            ClientType clientType,
            KeyType keyType,
            Collection<String> fields) {
        keyType = getKeyType(clientType, keyType);
        Map<String, WrapperReEncryptionKey> result = new HashMap<>();

        KeyPair clientKeyPair;
        try {
            clientKeyPair = KeyUtils.getECKeyPairFromPEM(clientKey);
        } catch (IOException e) {
            throw new CommonException(e);
        }

        for (String field : fields) {
            PrivateKey key = GranularUtils.deriveKeyFromData(masterKey, field);
            KeyPair masterKeyPair = new KeyPair(null, key);
            WrapperReEncryptionKey reEncryptionKey;
            switch (clientType) {
                case CONSUMER:
                    reEncryptionKey = getReEncryptionKey(
                            algorithm, masterKeyPair, clientKeyPair, curve, keyType);
                    break;
                case PRODUCER:
                    reEncryptionKey = getReEncryptionKey(
                            algorithm, clientKeyPair, masterKeyPair, curve, KeyType.PRIVATE);
                    break;
                default:
                    throw new CommonException("Unsupported client type %s", clientType);
            }
            result.put(field, reEncryptionKey);
        }

        return result;
    }

    private WrapperReEncryptionKey getReEncryptionKey(
            EncryptionAlgorithm algorithm,
            String masterKey,
            String clientKey,
            String curve,
            ClientType clientType,
            KeyType keyType) {
        keyType = getKeyType(clientType, keyType);
        switch (clientType) {
            case CONSUMER:
                return getReEncryptionKey(algorithm, masterKey, clientKey, curve, keyType);
            case PRODUCER:
                return getReEncryptionKey(algorithm, clientKey, masterKey, curve, KeyType.PRIVATE);
            default:
                throw new CommonException("Unsupported client type %s", clientType);
        }
    }

    private KeyType getKeyType(ClientType clientType, KeyType keyType) {
        if (clientType == ClientType.PRODUCER &&
                keyType != null &&
                keyType != KeyType.PRIVATE) {
            throw new CommonException("Only available '%s' key type for '%s' client type",
                    KeyType.PRIVATE, ClientType.PRODUCER);
        }
        if (keyType == null) {
            keyType = KeyType.DEFAULT;
        }
        return keyType;
    }

    private WrapperReEncryptionKey getReEncryptionKey(
            EncryptionAlgorithm algorithm,
            KeyPair keyPairFrom,
            KeyPair keyPairTo,
            String curve,
            KeyType keyType) {
        try {
            return KeyUtils.generateReEncryptionKey(algorithm, keyPairFrom, keyPairTo, keyType, curve);
        } catch (IOException | NoSuchAlgorithmException |
                InvalidKeyException | NoSuchProviderException e) {
            throw new CommonException(e);
        }
    }

    private WrapperReEncryptionKey getReEncryptionKey(
            EncryptionAlgorithm algorithm,
            String keyFrom,
            String keyTo,
            String curve,
            KeyType keyType) {
        try {
            return KeyUtils.generateReEncryptionKey(algorithm, keyFrom, keyTo, keyType, curve);
        } catch (IOException | NoSuchAlgorithmException |
                InvalidKeyException | NoSuchProviderException e) {
            throw new CommonException(e);
        }
    }

    /**
     * Delete the key from storage
     *
     * @param type        client type
     * @param clientName  client principal name
     * @param channelName channel which is used for the key. If null then key is
     *                    searched for all channels
     * @param field       the field in the data structure to which this key belongs.
     *                    If null and channel type is GRANULAR then will be deleted all subkeys
     */
    public void deleteReEncryptionKey(ClientType type, String clientName, String channelName, String field)
            throws CommonException {
        Channel channel = zooKeeperHandler.getChannel(channelName);
        if (channel != null && channel.getType() == EncryptionType.GRANULAR && field == null) {
            deleteGranularReEncryptionKeys(type, clientName, channel);
        } else {
            zooKeeperHandler.deleteKeyFromZooKeeper(channelName, clientName, type, field);
        }
        if (channelName != null && field != null) {
            LOGGER.info("Key for user '{}' with type '{}' for channel '{}' and field '{}' was deleted",
                    clientName, type, channelName, field);
        } else if (channelName != null) {
            LOGGER.info("Key for user '{}' with type '{}' for channel '{}' was deleted",
                    clientName, type, channelName);
        } else {
            LOGGER.info("Key for user '{}' with type '{}' was deleted", clientName, type);
        }
    }

    private void deleteGranularReEncryptionKeys(ClientType type, String clientName, Channel channel) {
        for (String channelField : channel.getFields()) {
            if (zooKeeperHandler.isKeyExists(channel.getName(), clientName, type, channelField)) {
                zooKeeperHandler.deleteKeyFromZooKeeper(channel.getName(), clientName, type, channelField);
            }
        }
    }

    /**
     * Print all keys or channels in the storage
     *
     * @param channels print channels (if true) or keys (if false)
     */
    public void listAll(boolean channels) {
        if (channels) {
            LOGGER.info("All channels:");
            for (Channel channel : zooKeeperHandler.listAllChannels()) {
                LOGGER.info(channel.toString());
            }
        } else {
            LOGGER.info("All re-encryption keys:");
            for (KeyHolder key : zooKeeperHandler.listAllKeys()) {
                LOGGER.info(key.toString());
            }
        }
    }

    /**
     * Create channel in the storage
     *
     * @param channel           channel name for creating
     * @param type              encryption type
     * @param accessorClassName accessor class name
     * @param format            data format
     */
    public void createChannel(
            String channel, EncryptionType type, String accessorClassName, DataFormat format) {
        Class<? extends StructuredDataAccessor> clazz;
        if (format == null) {
            clazz = getAccessorClass(accessorClassName);
        } else {
            clazz = format.getAccessorClass();
        }
        zooKeeperHandler.createChannelInZooKeeper(channel, type, clazz);
        LOGGER.info("Channel '{}' with type '{}' and accessor class '{}' was created",
                channel, type, clazz == null ? "none" : clazz.getName());
    }

    /**
     * Deleting channel in the storage
     *
     * @param channel channel name for deleting
     */
    public void deleteChannel(String channel) {
        zooKeeperHandler.deleteChannelFromZooKeeper(channel);
        LOGGER.info("Channel '{}' was deleted", channel);
    }

    /**
     * Generate key pair and write to the pem-files
     *
     * @param algorithm      encryption algorithm
     * @param curve          the name of the curve requested
     * @param privateKeyPath private key path
     * @param publicKeyPath  public key path (may be null)
     */
    public void generateKeyPair(EncryptionAlgorithm algorithm,
                                String curve,
                                String privateKeyPath,
                                String publicKeyPath) {
        try {
            KeyPair keyPair = KeyUtils.generateECKeyPair(algorithm, curve).getKeyPair();
            LOGGER.info("Key pair '{}' were generated", curve);
            KeyUtils.writeKeyPairToPEM(privateKeyPath, keyPair, KeyType.PRIVATE);
            LOGGER.info("Private key was saved to the path '{}'", privateKeyPath);
            if (publicKeyPath != null) {
                KeyUtils.writeKeyPairToPEM(publicKeyPath, keyPair, KeyType.PUBLIC);
                LOGGER.info("Public key was saved to the path '{}'", publicKeyPath);
            }
        } catch (IOException e) {
            throw new CommonException(e);
        }
    }
}
