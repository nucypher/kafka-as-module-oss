package com.nucypher.kafka.clients;

import com.google.common.base.Optional;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.nucypher.kafka.clients.granular.StructuredMessageHandler;
import com.nucypher.kafka.encrypt.DataEncryptionKeyManager;
import com.nucypher.kafka.errors.CommonException;
import com.nucypher.kafka.utils.WrapperReEncryptionKey;
import com.nucypher.kafka.zk.BaseZooKeeperHandler;
import com.nucypher.kafka.zk.Channel;
import com.nucypher.kafka.zk.ClientType;
import com.nucypher.kafka.zk.EncryptionType;
import com.nucypher.kafka.zk.KeyHolder;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Re-encryption handler for broker
 */
public class ReEncryptionHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReEncryptionHandler.class);

    private final BaseZooKeeperHandler zkHandler;
    private final MessageHandler messageHandler;

    private final int reEncryptionKeysCacheCapacity;
    private final long reEncryptionKeysCacheTTLms;
    private final int granularReEncryptionKeysCacheCapacity;
    private final long granularReEncryptionKeysCacheTTLms;
    private final int channelsCacheCapacity;
    private final long channelsCacheTTLms;

    private LoadingCache<ReEncryptionKeysCacheKey, Optional<KeyHolder>> reEncryptionKeysCache;
    private LoadingCache<GranularReEncryptionKeysCacheKey, Optional<KeyHolder>> granularReEncryptionKeysCache;
    private LoadingCache<String, Optional<Channel>> channelsCache;

    private static class ReEncryptionKeysCacheKey {
        private String channel;
        private String name;
        private ClientType clientType;

        public ReEncryptionKeysCacheKey(String channel, String name, ClientType clientType) {
            this.channel = channel;
            this.name = name;
            this.clientType = clientType;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ReEncryptionKeysCacheKey that = (ReEncryptionKeysCacheKey) o;
            return Objects.equals(channel, that.channel) &&
                    Objects.equals(name, that.name) &&
                    clientType == that.clientType;
        }

        @Override
        public int hashCode() {
            return Objects.hash(channel, name, clientType);
        }
    }

    private static class GranularReEncryptionKeysCacheKey {
        private String channel;
        private String name;
        private ClientType clientType;
        private String field;

        public GranularReEncryptionKeysCacheKey(
                String channel, String name, ClientType clientType, String field) {
            this.channel = channel;
            this.name = name;
            this.clientType = clientType;
            this.field = field;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            GranularReEncryptionKeysCacheKey that = (GranularReEncryptionKeysCacheKey) o;
            return Objects.equals(channel, that.channel) &&
                    Objects.equals(name, that.name) &&
                    clientType == that.clientType &&
                    Objects.equals(field, that.field);
        }

        @Override
        public int hashCode() {
            return Objects.hash(channel, name, clientType, field);
        }
    }

    /**
     * @param zooKeeperHost                         ZooKeeper address
     * @param keysRootPath                          root path for keys in ZooKeeper
     * @param edekCacheCapacity                     EDEK cache capacity
     * @param reEncryptionKeysCacheCapacity         re-encryption keys cache capacity
     * @param reEncryptionKeysCacheTTLms            re-encryption keys cache TTL in ms
     * @param granularReEncryptionKeysCacheCapacity granular re-encryption keys cache capacity
     * @param granularReEncryptionKeysCacheTTLms    granular re-encryption keys cache TTL in ms
     * @param channelsCacheCapacity                 channels cache capacity
     * @param channelsCacheTTLms                    channels cache TTL in ms
     */
    public ReEncryptionHandler(String zooKeeperHost,
                               String keysRootPath,
                               Integer edekCacheCapacity,
                               int reEncryptionKeysCacheCapacity,
                               long reEncryptionKeysCacheTTLms,
                               int granularReEncryptionKeysCacheCapacity,
                               long granularReEncryptionKeysCacheTTLms,
                               int channelsCacheCapacity,
                               long channelsCacheTTLms) {
        this(new BaseZooKeeperHandler(zooKeeperHost, keysRootPath),
                edekCacheCapacity,
                reEncryptionKeysCacheCapacity,
                reEncryptionKeysCacheTTLms,
                granularReEncryptionKeysCacheCapacity,
                granularReEncryptionKeysCacheTTLms,
                channelsCacheCapacity,
                channelsCacheTTLms);
    }

    /**
     * @param zooKeeperClient                       ZooKeeper client
     * @param keysRootPath                          root path for keys in ZooKeeper
     * @param edekCacheCapacity                     EDEK cache capacity
     * @param reEncryptionKeysCacheCapacity         re-encryption keys cache capacity
     * @param reEncryptionKeysCacheTTLms            re-encryption keys cache TTL in ms
     * @param granularReEncryptionKeysCacheCapacity granular re-encryption keys cache capacity
     * @param granularReEncryptionKeysCacheTTLms    granular re-encryption keys cache TTL in ms
     * @param channelsCacheCapacity                 channels cache capacity
     * @param channelsCacheTTLms                    channels cache TTL in ms
     */
    public ReEncryptionHandler(ZooKeeper zooKeeperClient,
                               String keysRootPath,
                               Integer edekCacheCapacity,
                               int reEncryptionKeysCacheCapacity,
                               long reEncryptionKeysCacheTTLms,
                               int granularReEncryptionKeysCacheCapacity,
                               long granularReEncryptionKeysCacheTTLms,
                               int channelsCacheCapacity,
                               long channelsCacheTTLms) {
        this(new BaseZooKeeperHandler(zooKeeperClient, keysRootPath),
                edekCacheCapacity,
                reEncryptionKeysCacheCapacity,
                reEncryptionKeysCacheTTLms,
                granularReEncryptionKeysCacheCapacity,
                granularReEncryptionKeysCacheTTLms,
                channelsCacheCapacity,
                channelsCacheTTLms);
    }

    /**
     * @param zkHandler                             ZooKeeper handler
     * @param edekCacheCapacity                     EDEK cache capacity
     * @param reEncryptionKeysCacheCapacity         re-encryption keys cache capacity
     * @param reEncryptionKeysCacheTTLms            re-encryption keys cache TTL in ms
     * @param granularReEncryptionKeysCacheCapacity granular re-encryption keys cache capacity
     * @param granularReEncryptionKeysCacheTTLms    granular re-encryption keys cache TTL in ms
     * @param channelsCacheCapacity                 channels cache capacity
     * @param channelsCacheTTLms                    channels cache TTL in ms
     */
    public ReEncryptionHandler(BaseZooKeeperHandler zkHandler,
                               Integer edekCacheCapacity,
                               int reEncryptionKeysCacheCapacity,
                               long reEncryptionKeysCacheTTLms,
                               int granularReEncryptionKeysCacheCapacity,
                               long granularReEncryptionKeysCacheTTLms,
                               int channelsCacheCapacity,
                               long channelsCacheTTLms) {
        this.zkHandler = zkHandler;
        DataEncryptionKeyManager keyManager = new DataEncryptionKeyManager(edekCacheCapacity);
        messageHandler = new MessageHandler(keyManager);
        this.reEncryptionKeysCacheCapacity = reEncryptionKeysCacheCapacity;
        this.granularReEncryptionKeysCacheCapacity = granularReEncryptionKeysCacheCapacity;
        this.channelsCacheCapacity = channelsCacheCapacity;
        this.channelsCacheTTLms = channelsCacheTTLms;
        this.reEncryptionKeysCacheTTLms = reEncryptionKeysCacheTTLms;
        this.granularReEncryptionKeysCacheTTLms = granularReEncryptionKeysCacheTTLms;
    }

    /**
     * @param zooKeeperHost ZooKeeper address
     * @param config        {@link ReEncryptionHandler} configuration
     */
    public ReEncryptionHandler(String zooKeeperHost,
                               AbstractConfig config) {
        String keysRootPath = config.getString(
                ReEncryptionHandlerConfigs.RE_ENCRYPTION_KEYS_PATH);
        this.zkHandler = new BaseZooKeeperHandler(zooKeeperHost, keysRootPath);
        DataEncryptionKeyManager keyManager = new DataEncryptionKeyManager(
                config.getInt(ReEncryptionHandlerConfigs.EDEK_CACHE_CAPACITY));
        messageHandler = new MessageHandler(keyManager);
        this.reEncryptionKeysCacheCapacity = config.getInt(
                ReEncryptionHandlerConfigs.RE_ENCRYPTION_KEYS_CACHE_CAPACITY);
        this.granularReEncryptionKeysCacheCapacity = config.getInt(
                ReEncryptionHandlerConfigs.GRANULAR_RE_ENCRYPTION_KEYS_CACHE_CAPACITY);
        this.channelsCacheCapacity = config.getInt(
                ReEncryptionHandlerConfigs.CHANNELS_CACHE_CAPACITY);
        this.channelsCacheTTLms = config.getLong(
                ReEncryptionHandlerConfigs.CHANNELS_CACHE_TTL_MS);
        this.reEncryptionKeysCacheTTLms = config.getLong(
                ReEncryptionHandlerConfigs.RE_ENCRYPTION_KEYS_CACHE_TTL_MS);
        this.granularReEncryptionKeysCacheTTLms = config.getLong(
                ReEncryptionHandlerConfigs.GRANULAR_RE_ENCRYPTION_KEYS_CACHE_TTL_MS);
    }

    private void initializeReEncryptionKeysCache() {
        if (reEncryptionKeysCache != null) {
            return;
        }
        reEncryptionKeysCache = CacheBuilder.newBuilder()
                .maximumSize(reEncryptionKeysCacheCapacity)
                .expireAfterWrite(reEncryptionKeysCacheTTLms, TimeUnit.MILLISECONDS)
                .build(new CacheLoader<ReEncryptionKeysCacheKey, Optional<KeyHolder>>() {
                    public Optional<KeyHolder> load(ReEncryptionKeysCacheKey key) {
                        KeyHolder keyHolder = zkHandler.getKey(
                                key.channel, key.name, key.clientType);
                        if (keyHolder == null || keyHolder.isExpired()) {
                            return Optional.absent();
                        }
                        return Optional.of(keyHolder);
                    }
                });
        LOGGER.debug("Re-encryption keys cache was initialized using capacity {}",
                reEncryptionKeysCacheCapacity);
    }

    private void initializeGranularReEncryptionKeysCache() {
        if (granularReEncryptionKeysCache != null) {
            return;
        }
        granularReEncryptionKeysCache = CacheBuilder.newBuilder()
                .maximumSize(granularReEncryptionKeysCacheCapacity)
                .expireAfterWrite(granularReEncryptionKeysCacheTTLms, TimeUnit.MILLISECONDS)
                .build(new CacheLoader<GranularReEncryptionKeysCacheKey, Optional<KeyHolder>>() {
                    public Optional<KeyHolder> load(GranularReEncryptionKeysCacheKey key) {
                        KeyHolder keyHolder = zkHandler.getKey(
                                key.channel, key.name, key.clientType, key.field);
                        if (keyHolder == null || keyHolder.isExpired()) {
                            return Optional.absent();
                        }
                        return Optional.of(keyHolder);
                    }
                });
        LOGGER.debug("Granular re-encryption keys cache was initialized using capacity {}",
                granularReEncryptionKeysCacheCapacity);
    }

    private void initializeChannelsCache() {
        if (channelsCache != null) {
            return;
        }
        channelsCache = CacheBuilder.newBuilder()
                .maximumSize(channelsCacheCapacity)
                .expireAfterWrite(channelsCacheTTLms, TimeUnit.MILLISECONDS)
                .build(new CacheLoader<String, Optional<Channel>>() {
                    public Optional<Channel> load(String key) {
                        return Optional.fromNullable(zkHandler.getChannel(key));
                    }
                });
        LOGGER.debug("Channels cache was initialized using capacity {} and TTL {}",
                channelsCacheCapacity, channelsCacheTTLms);
    }

    private KeyHolder getKey(String channel, String name, ClientType clientType) {
        initializeReEncryptionKeysCache();
        try {
            ReEncryptionKeysCacheKey cacheKey =
                    new ReEncryptionKeysCacheKey(channel, name, clientType);
            Optional<KeyHolder> keyHolder = reEncryptionKeysCache.get(cacheKey);
            // if key for full re-encryption is absent
            // then we should check it each time so cache doesn't hold null values
            if (!keyHolder.isPresent() || keyHolder.get().isExpired()) {
                reEncryptionKeysCache.invalidate(cacheKey);
                return null;
            }
            return keyHolder.get();
        } catch (ExecutionException e) {
            throw new CommonException(e);
        }
    }

    private KeyHolder getKey(String channel, String name, ClientType clientType, String field) {
        initializeGranularReEncryptionKeysCache();
        try {
            GranularReEncryptionKeysCacheKey cacheKey =
                    new GranularReEncryptionKeysCacheKey(channel, name, clientType, field);
            Optional<KeyHolder> keyHolder = granularReEncryptionKeysCache.get(cacheKey);
            // if field key for granular re-encryption is absent
            // then we just skip this field
            // so we should hold empty values to not check key every time
            if (keyHolder.isPresent() && keyHolder.get().isExpired()) {
                granularReEncryptionKeysCache.invalidate(cacheKey);
            }
            return keyHolder.orNull();
        } catch (ExecutionException e) {
            throw new CommonException(e);
        }
    }

    private Channel getChannel(String channel) {
        initializeChannelsCache();
        try {
            return channelsCache.get(channel).orNull();
        } catch (ExecutionException e) {
            throw new CommonException(e);
        }
    }

    /**
     * Checks topic encryption
     *
     * @param topic         topic to check
     * @param principalName Kafka principal name
     * @param clientType    client type
     * @return result of checking
     */
    public boolean isTopicEncrypted(String topic, String principalName, ClientType clientType) {
        Channel channel = getChannel(topic);
        if (channel == null) {
            return false;
        }
        if (channel.getType() == EncryptionType.GRANULAR) {
            return true;
        }
        KeyHolder keyHolder = getKey(topic, principalName, clientType);
        return keyHolder == null || keyHolder.isExpired() ||
                keyHolder.getKey() != null && !keyHolder.getKey().isEmpty();
    }

    /**
     * Checks rights for records re-encryption
     *
     * @param topic         topic name to check
     * @param principalName principal name to check
     * @param clientType    client type to check
     * @return result of checking
     */
    public boolean isAllowReEncryption(String topic,
                                       String principalName,
                                       ClientType clientType) {
        Channel channel = getChannel(topic);
        if (channel.getType() == EncryptionType.GRANULAR) {
            return true;
        }
        KeyHolder keyHolder = getKey(topic, principalName, clientType);
        return keyHolder != null && !keyHolder.isExpired();
    }

    /**
     * Re-encrypt payload in one topic
     *
     * @param payload       payload
     * @param topic         topic name
     * @param principalName user principal name
     * @param clientType    client type
     * @return re-encrypted payload
     */
    public byte[] reEncryptTopic(byte[] payload,
                                 String topic,
                                 String principalName,
                                 ClientType clientType) {
        Channel channel = getChannel(topic);
        switch (channel.getType()) {
            case FULL:
                return fullReEncryptTopic(payload, channel, principalName, clientType);
            case GRANULAR:
                return granularReEncryptTopic(payload, channel, principalName, clientType);
            default:
                return payload;
        }
    }


    /**
     * Full re-encrypt payload in one topic
     *
     * @param payload       payload
     * @param channel       channel object
     * @param principalName user principal name
     * @param clientType    client type
     * @return re-encrypted payload
     */
    public byte[] fullReEncryptTopic(byte[] payload,
                                     Channel channel,
                                     String principalName,
                                     ClientType clientType) {
        String topic = channel.getName();
        KeyHolder keyHolder = getKey(topic, principalName, clientType);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Re-encrypt records in the topic '{}' for {} '{}'",
                    topic, clientType.toString().toLowerCase(), principalName);
        }
        return messageHandler.reEncrypt(topic, payload, keyHolder.getKey());
    }


    /**
     * Granular re-encrypt payload from record
     *
     * @param payload       structured payload
     * @param channel       channel object
     * @param principalName principal name
     * @param clientType    client type
     * @return re-encrypted payload
     */
    public byte[] granularReEncryptTopic(byte[] payload,
                                         Channel channel,
                                         String principalName,
                                         ClientType clientType) {
        String topic = channel.getName();
        StructuredMessageHandler structuredMessageHandler =
                new StructuredMessageHandler(messageHandler);
        Map<String, WrapperReEncryptionKey> reKeys = new HashMap<>();
        for (String field : structuredMessageHandler.getAllEncryptedFields(payload)) {
            KeyHolder keyHolder = getKey(topic, principalName, clientType, field);
            if (keyHolder == null && clientType == ClientType.PRODUCER) {
                throw new CommonException("There are no re-encryption key " +
                        "for %s '%s', channel '%s' and field '%s'",
                        clientType.toString().toLowerCase(), principalName, topic, field);
            } else if (keyHolder == null || keyHolder.getKey().isEmpty()) {
                //TODO change to trace
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Field '{}' in the message in the topic '{}' " +
                                    "for {} '{}' is not re-encrypted",
                            field, topic, clientType.toString().toLowerCase(), principalName);
                }
            } else {
                reKeys.put(field, keyHolder.getKey());
            }
        }
        return structuredMessageHandler.reEncrypt(topic, reKeys);
    }

}
