package com.nucypher.kafka.zk;

import com.nucypher.kafka.clients.granular.StructuredDataAccessor;
import com.nucypher.kafka.errors.CommonException;
import com.nucypher.kafka.utils.GranularUtils;
import com.nucypher.kafka.utils.WrapperReEncryptionKey;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.common.PathUtils;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Base class for working with ZooKeeper
 *
 * @author szotov
 */
public class BaseZooKeeperHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(BaseZooKeeperHandler.class);
    private static final String NAME_DELIMITER = "-";
    private static final String CHANNEL_POSTFIX = "channel";
    private static final String EXPIRED_NAME = "expired";
    private static final String FIELD_POSTFIX = "field";

    /**
     * ZooKeeper path separator
     */
    public static final String ZOOKEEPER_PATH_SEPARATOR = "/";

    /**
     * {@link ZooKeeper} client connection
     */
    protected ZooKeeper zooKeeperClient;
    /**
     * Root path with keys
     */
    protected String keysRootPath;

    /**
     * Create new instance with existing connection to ZooKeeper
     *
     * @param zooKeeperClient {@link ZooKeeper} instance
     * @param keysRootPath    root path with keys
     */
    public BaseZooKeeperHandler(ZooKeeper zooKeeperClient, String keysRootPath) {
        this.keysRootPath = keysRootPath;
        PathUtils.validatePath(keysRootPath);
        this.zooKeeperClient = zooKeeperClient;
    }

    /**
     * Lazy constructor
     */
    protected BaseZooKeeperHandler() {

    }

    /**
     * @param type       client type
     * @param clientName client name
     * @return full key name
     */
    protected static String getKeyName(ClientType type, String clientName) {
        return ZOOKEEPER_PATH_SEPARATOR + clientName + NAME_DELIMITER + type.toString().toLowerCase();
    }

    /**
     * @param channel channel name
     * @return channel path
     */
    protected static String getChannelPath(String channel) {
        return ZOOKEEPER_PATH_SEPARATOR + channel + NAME_DELIMITER + CHANNEL_POSTFIX;
    }

    /**
     * @return expired postfix
     */
    protected static String getExpiredPath() {
        return NAME_DELIMITER + EXPIRED_NAME;
    }

    /**
     * @param fieldName field name
     * @return field path
     */
    protected static String getFieldPath(String fieldName) {
        StringBuilder builder = new StringBuilder();
        for (String part : GranularUtils.parsePath(fieldName)) {
            builder.append(ZOOKEEPER_PATH_SEPARATOR)
                    .append(part).append(NAME_DELIMITER).append(FIELD_POSTFIX);
        }

        return builder.toString();
    }

    /**
     * Get all keys from ZooKeeper
     *
     * @return {@link KeyHolder} list
     * @throws CommonException if problem with ZooKeeper or problem with parsing data
     */
    public Set<KeyHolder> listAllKeys() throws CommonException {
        try {
            Set<KeyHolder> result = listAllKeys(new HashSet<KeyHolder>(), keysRootPath);
            LOGGER.info("Found {} keys", result.size());
            return result;
        } catch (KeeperException | InterruptedException | IOException e) {
            throw new CommonException(e);
        }
    }

    private Set<KeyHolder> listAllKeys(Set<KeyHolder> keys, String rootPath)
            throws KeeperException, InterruptedException, IOException {
        List<String> keysAndChannels = zooKeeperClient.getChildren(rootPath, false);
        List<String> channels = new ArrayList<>();
        for (String value : keysAndChannels) {
            String path = rootPath + ZOOKEEPER_PATH_SEPARATOR + value;
            if (isValidChannelName(value)) {
                channels.add(value);
            } else if (isValidKeyName(value)) {
                keys.add(createKeyHolder(null, path, value, null));
            } else if (isAccessible(path)) {
                listAllKeys(keys, path);
            }
        }
        for (String channel : channels) {
            listAllKeys(keys, rootPath, channel);
        }
        return keys;
    }

    private Set<KeyHolder> listAllKeys(Set<KeyHolder> keys, String rootPath, String channel)
            throws KeeperException, InterruptedException, IOException {
        String path = rootPath + ZOOKEEPER_PATH_SEPARATOR + channel;
        List<String> children = zooKeeperClient.getChildren(path, false);
        for (String child : children) {
            String childPath = path + ZOOKEEPER_PATH_SEPARATOR + child;
            if (isValidKeyName(child)) {
                keys.add(createKeyHolder(channel, childPath, child, null));
            } else if (isValidFieldName(child)) {
                listAllKeys(keys, childPath, channel, extractDelimitedName(child));
            }
        }
        return keys;
    }

    private Set<KeyHolder> listAllKeys(
            Set<KeyHolder> keys, String rootPath, String channel, String fieldName)
            throws KeeperException, InterruptedException, IOException {
        List<String> keysAndFields = zooKeeperClient.getChildren(rootPath, false);
        for (String value : keysAndFields) {
            String fullPath = rootPath + ZOOKEEPER_PATH_SEPARATOR + value;
            if (isValidKeyName(value)) {
                keys.add(createKeyHolder(channel, fullPath, value, fieldName));
                continue;
            } else if (!isValidFieldName(value)) {
                continue;
            }

            listAllKeys(keys, fullPath, channel,
                    GranularUtils.getFieldName(fieldName, extractDelimitedName(value)));
        }
        return keys;
    }

    /**
     * Get all channels from ZooKeeper
     *
     * @return collection of channels
     * @throws CommonException if problem with ZooKeeper
     */
    public Set<Channel> listAllChannels() throws CommonException {
        try {
            Set<Channel> result = listAllChannels(keysRootPath);
            LOGGER.info("Found {} channels", result.size());
            return result;
        } catch (KeeperException | InterruptedException | ClassNotFoundException e) {
            throw new CommonException(e);
        }
    }

    private Set<Channel> listAllChannels(String rootPath)
            throws KeeperException, InterruptedException, ClassNotFoundException {
        List<String> keysAndChannels = zooKeeperClient.getChildren(rootPath, false);
        Set<Channel> channels = new HashSet<>();
        for (String value : keysAndChannels) {
            String fullPath = rootPath + ZOOKEEPER_PATH_SEPARATOR + value;
            boolean isChannel = isValidChannelName(value);
            if (!isChannel && !isValidKeyName(value) && isAccessible(fullPath)) {
                channels.addAll(listAllChannels(fullPath));
                continue;
            } else if (!isChannel) {
                continue;
            }

            String name = extractDelimitedName(value);
            channels.add(createChannel(fullPath, name));
        }
        return channels;
    }

    private Channel createChannel(String fullPath, String name)
            throws KeeperException, InterruptedException, ClassNotFoundException {
        try {
            byte[] channelBytes = zooKeeperClient.getData(fullPath, false, new Stat());
            EncryptionType type = getEncryptionType(channelBytes);
            Set<String> fields = new HashSet<>();
            Class<? extends StructuredDataAccessor> clazz = null;
            if (type == EncryptionType.GRANULAR) {
                fields = listAllFields(fields, fullPath, "");
                clazz = getAccessorClass(channelBytes);
            }
            return new Channel(name, type, fields, clazz);
        } catch (ClassNotFoundException e) {
            throw new ClassNotFoundException(String.format(
                    "Accessor class '%s' not found for channel '%s'", e.getMessage(), name));
        }
    }

    private Set<String> listAllFields(Set<String> fields, String rootPath, String parentFieldName)
            throws KeeperException, InterruptedException {
        List<String> keysAndFields = zooKeeperClient.getChildren(rootPath, false);
        for (String value : keysAndFields) {
            if (!isValidFieldName(value)) {
                continue;
            }
            String fullPath = rootPath + ZOOKEEPER_PATH_SEPARATOR + value;
            String fieldName =
                    GranularUtils.getFieldName(parentFieldName, extractDelimitedName(value));
            fields.add(fieldName);
            listAllFields(fields, fullPath, fieldName);
        }
        return fields;
    }

    /**
     * Get accessor class from channel bytes
     *
     * @param channelBytes channel bytes
     * @return accessor class
     * @throws ClassNotFoundException if accessor class not found
     */
    @SuppressWarnings("unchecked")
    protected static Class<? extends StructuredDataAccessor> getAccessorClass(byte[] channelBytes)
            throws ClassNotFoundException {
        if (channelBytes == null || channelBytes.length <= 1) {
            return null;
        }

        byte[] classBytes = Arrays.copyOfRange(channelBytes, 1, channelBytes.length);
        String className = new String(classBytes);
        return (Class<? extends StructuredDataAccessor>) Class.forName(className);
    }

    /**
     * Get channel encryption type
     *
     * @param channelBytes channel bytes
     * @return encryption type
     * @throws KeeperException      if problem with ZooKeeper
     * @throws InterruptedException if problem with ZooKeeper
     */
    protected static EncryptionType getEncryptionType(byte[] channelBytes)
            throws KeeperException, InterruptedException {
        if (channelBytes == null || channelBytes.length == 0) {
            return EncryptionType.FULL;
        }
        EncryptionType type = EncryptionType.valueOf(channelBytes[0]);
        if (type == null) {
            type = EncryptionType.FULL;
        }
        return type;
    }

    private static boolean isValidKeyName(String path) {
        if (!path.contains(NAME_DELIMITER)) {
            return false;
        }
        String[] parts = path.split(NAME_DELIMITER);

        return parts.length >= 2 && ClientType.contains(parts[parts.length - 1].toUpperCase());
    }

    private static boolean isValidChannelName(String path) {
        return path.endsWith(CHANNEL_POSTFIX);
    }

    private static boolean isValidFieldName(String path) {
        return path.endsWith(FIELD_POSTFIX);
    }

    /**
     * Checks path accessible
     *
     * @param path path to check
     * @return result of checking
     * @throws InterruptedException If the server transaction is interrupted.
     * @throws KeeperException      If the server signals an error with a non-zero error code.
     */
    protected boolean isAccessible(String path) throws InterruptedException, KeeperException {
        try {
            zooKeeperClient.getChildren(path, false);
            return true;
        } catch (KeeperException ke) {
            if (Code.NOAUTH.equals(ke.code())) {
                return false;
            } else {
                throw ke;
            }
        }
    }

    private KeyHolder createKeyHolder(String channel, String rootPath, String name, String field)
            throws KeeperException, InterruptedException, IOException {
        String[] parts = name.split(NAME_DELIMITER);
        StringBuilder builder = new StringBuilder(parts[0]);
        for (int i = 1; i < parts.length - 1; i++) {
            builder.append(NAME_DELIMITER).append(parts[i]);
        }
        String userName = builder.toString();
        ClientType type = ClientType.valueOf(parts[parts.length - 1].toUpperCase());
        String channelName = extractDelimitedName(channel);
        String fieldName = extractDelimitedName(field);

        return createKeyHolder(rootPath, userName, type, channelName, fieldName);
    }

    private static String extractDelimitedName(String inputName) {
        String name = null;
        if (inputName != null) {
            String[] parts = inputName.split(NAME_DELIMITER);
            StringBuilder builder = new StringBuilder(parts[0]);
            for (int i = 1; i < parts.length - 1; i++) {
                builder.append(NAME_DELIMITER).append(parts[i]);
            }
            name = builder.toString();
        }
        return name;
    }

    private KeyHolder createKeyHolder(
            String fullPath, String userName, ClientType type, String channelName, String fieldName)
            throws KeeperException, InterruptedException, IOException {
        Long expired = null;
        String expiredPath = fullPath + getExpiredPath();
        if (zooKeeperClient.exists(expiredPath, false) != null) {
            byte[] expiredBytes = zooKeeperClient.getData(expiredPath, false, new Stat());
            expired = new BigInteger(expiredBytes).longValue();
        }

        WrapperReEncryptionKey keyData = null;
        if (isAccessible(fullPath)) {
            keyData = WrapperReEncryptionKey.getInstance(
                    zooKeeperClient.getData(fullPath, false, new Stat()));
        }
        return KeyHolder.builder()
                .setChannel(channelName)
                .setName(userName)
                .setType(type)
                .setKey(keyData)
                .setExpiredDate(expired)
                .setField(fieldName)
                .build();
    }

    /**
     * Get key from ZooKeeper
     *
     * @param channel channel which is used for the key. If null then key is
     *                searched for all channels
     * @param name    the key name
     * @param type    client type
     * @return {@link KeyHolder} if key exists or null if not
     * @throws CommonException if problem with ZooKeeper or problem with parsing data
     */
    public KeyHolder getKey(String channel, String name, ClientType type)
            throws CommonException {
        return getKey(channel, name, type, null);
    }

    /**
     * Get key from ZooKeeper
     *
     * @param channel   channel which is used for the key. If null then key is
     *                  searched for all channels
     * @param name      the key name
     * @param type      client type
     * @param fieldName field name which is used for the key
     * @return {@link KeyHolder} if key exists or null if not
     * @throws CommonException if problem with ZooKeeper or problem with parsing data
     */
    public KeyHolder getKey(
            String channel, String name, ClientType type, String fieldName)
            throws CommonException {
        try {
            KeyHolder key = getKey(keysRootPath, channel, name, type, fieldName);
            if (key == null || key.getKey() == null) {
                LOGGER.warn("Key for user '{}' with type '{}', channel '{}' and field '{}' was not found",
                        name, type, channel, fieldName == null ? "all" : fieldName);
                return null;
            }
            if (key.isExpired()) {
                LOGGER.warn("Key for user '{}' with type '{}', channel '{}' and field '{}' is expired",
                        name, type, channel, fieldName == null ? "all" : fieldName);
//				return null;
            } else {
                LOGGER.debug("Key for user '{}' with type '{}', channel '{}' and field '{}' was found",
                        name, type, channel, fieldName == null ? "all" : fieldName);
            }
            return key;
        } catch (KeeperException | InterruptedException | IOException e) {
            throw new CommonException(e);
        }
    }

    private KeyHolder getKey(
            String rootPath, String channel, String name, ClientType type, String fieldName)
            throws KeeperException, InterruptedException, IOException { //TODO need expired checking
        String keyName = getKeyName(type, name);
        String fullKeyPath = rootPath;
        if (channel != null) {
            fullKeyPath += getChannelPath(channel);
        }
        if (fieldName != null) {
            fullKeyPath += getFieldPath(fieldName);
        }
        fullKeyPath += keyName;
        if (zooKeeperClient.exists(fullKeyPath, false) != null) {
            return createKeyHolder(fullKeyPath, name, type, channel, fieldName);
        }

        for (String child : zooKeeperClient.getChildren(rootPath, false)) {
            String path = rootPath + ZOOKEEPER_PATH_SEPARATOR + child;
            if (isValidChannelName(child) || isValidKeyName(child) ||
                    isValidFieldName(child) || !isAccessible(path)) {
                continue;
            }
            KeyHolder key = getKey(path, channel, name, type, fieldName);
            if (key != null) {
                return key;
            }
        }
        return null;
    }

    /**
     * Checks if a key exists
     *
     * @param channel   channel which is used for the key. If null then key is
     *                  searched for all channels
     * @param name      the key name
     * @param type      client type
     * @param fieldName field name which is used for the key
     * @return result of checking
     * @throws CommonException if problem with ZooKeeper or problem with parsing data
     */
    public boolean isKeyExists(String channel, String name, ClientType type, String fieldName)
            throws CommonException {
        try {
            return getKey(keysRootPath, channel, name, type, fieldName) != null;
        } catch (KeeperException | InterruptedException | IOException e) {
            throw new CommonException(e);
        }
    }

    /**
     * Checks if a key exists
     *
     * @param channel channel which is used for the key. If null then key is
     *                searched for all channels
     * @param name    the key name
     * @param type    client type
     * @return result of checking
     * @throws CommonException if problem with ZooKeeper or problem with parsing data
     */
    public boolean isKeyExists(String channel, String name, ClientType type)
            throws CommonException {
        return isKeyExists(channel, name, type, null);
    }

    /**
     * Checks existence of channel
     *
     * @param channelName channel name to check
     * @return result of checking
     * @throws CommonException if problem with ZooKeeper
     */
    public boolean isChannelExists(String channelName) throws CommonException {
        try {
            if (getChannel(keysRootPath, channelName) == null) {
                LOGGER.debug("Channel '{}' not found", channelName);
                return false;
            } else {
                LOGGER.debug("Channel '{}' was found", channelName);
                return true;
            }
        } catch (KeeperException | InterruptedException | ClassNotFoundException e) {
            throw new CommonException(e);
        }
    }

    private Channel getChannel(String rootPath, String channelName)
            throws KeeperException, InterruptedException, ClassNotFoundException {
        String fullChannelPath = rootPath + getChannelPath(channelName);
        if (zooKeeperClient.exists(fullChannelPath, false) != null) {
            return createChannel(fullChannelPath, channelName);
        }
        if (zooKeeperClient.exists(rootPath, false) == null) {
            return null;
        }

        for (String child : zooKeeperClient.getChildren(rootPath, false)) {
            String path = rootPath + ZOOKEEPER_PATH_SEPARATOR + child;
            if (isValidChannelName(child) || isValidKeyName(child) ||
                    isValidFieldName(child) || !isAccessible(path)) {
                continue;
            }
            Channel channel = getChannel(path, channelName);
            if (channel != null) {
                return channel;
            }
        }
        return null;
    }

    /**
     * Get information about channel (without fields)
     *
     * @param channelName channel name
     * @return channel
     * @throws CommonException if problem with ZooKeeper
     */
    public Channel getChannel(String channelName) throws CommonException {
        try {
            Channel channel = getChannel(keysRootPath, channelName);
            if (channel == null) {
                LOGGER.debug("Channel '{}' not found", channelName);
            } else {
                LOGGER.debug("Channel '{}' with type '{}' was found",
                        channelName, channel.getType());
            }
            return channel;
        } catch (KeeperException | InterruptedException | ClassNotFoundException e) {
            throw new CommonException(e);
        }
    }

}
