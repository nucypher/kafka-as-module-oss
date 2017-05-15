package com.nucypher.kafka.admin;

import com.nucypher.kafka.clients.granular.StructuredDataAccessor;
import com.nucypher.kafka.errors.CommonException;
import com.nucypher.kafka.zk.BaseZooKeeperHandler;
import com.nucypher.kafka.zk.ClientType;
import com.nucypher.kafka.zk.EncryptionType;
import com.nucypher.kafka.zk.KeyHolder;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.common.PathUtils;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.math.BigInteger;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Stream;

/**
 * Class for handling administration requests to ZooKeeper
 *
 * @author szotov
 */
public class AdminZooKeeperHandler extends BaseZooKeeperHandler implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(AdminZooKeeperHandler.class);
    private static final String DEFAULT_ZOOKEEPER_HOST_PORT = "localhost:2181";
    private static final String SCHEME_DIGEST = "digest";
    private static final String SCHEME_SASL = "sasl";

    private static final String PROPERTY_ADMIN_USER = "admin.user";
    private static final String PROPERTY_ADMIN_PASSWORD = "admin.password";
    private static final String PROPERTY_ADMIN_SCHEME = "admin.scheme";
    private static final String PROPERTY_KAFKA_USER = "kafka.user";
    private static final String PROPERTY_KAFKA_SCHEME = "kafka.scheme";
    private static final String PROPERTY_KEYS_ROOT_PATH = "keys.path";
    private static final String PROPERTY_ZOOKEEPER_HOST_PORT = "zookeeper.server";

    private String adminScheme;
    private String adminUser;
    private String adminPassword;
    private String kafkaScheme;
    private String kafkaUser;

    /**
     * Create new instance with new connection to ZooKeeper
     *
     * @param properties {@link Properties}
     * @throws CommonException if error with properties or
     *                         if connection to ZooKeeper cannot be established or
     *                         if problem with key root path in ZooKeeper
     */
    public AdminZooKeeperHandler(Properties properties) throws CommonException {
        String zooKeeperHostPort = properties.getProperty(PROPERTY_ZOOKEEPER_HOST_PORT,
                DEFAULT_ZOOKEEPER_HOST_PORT);
        adminScheme = properties.getProperty(PROPERTY_ADMIN_SCHEME);
        adminUser = properties.getProperty(PROPERTY_ADMIN_USER);
        adminPassword = properties.getProperty(PROPERTY_ADMIN_PASSWORD);
        kafkaScheme = properties.getProperty(PROPERTY_KAFKA_SCHEME);
        kafkaUser = properties.getProperty(PROPERTY_KAFKA_USER);
        keysRootPath = properties.getProperty(PROPERTY_KEYS_ROOT_PATH);
        validateValues();
        try {
            zooKeeperClient = getZooKeeperClient(
                    zooKeeperHostPort, adminScheme, adminUser, adminPassword);
            makeInsecurePath(keysRootPath);
            checkOrCreateChannel(keysRootPath, null, null);
        } catch (IOException | InterruptedException | KeeperException | ClassNotFoundException e) {
            throw new CommonException(e);
        }
    }

    private void validateValues() {
        String format = "%s must not be null";
        if (adminScheme == null) {
            throw new CommonException(format, PROPERTY_ADMIN_SCHEME);
        }
        if (!SCHEME_DIGEST.equals(adminScheme) && !SCHEME_SASL.equals(adminScheme)) {
            throw new CommonException(
                    "Scheme '%s' is wrong. Only '%s' and '%s' are available for %s",
                    adminScheme, SCHEME_DIGEST, SCHEME_SASL, PROPERTY_ADMIN_SCHEME);
        }
        if (SCHEME_DIGEST.equals(adminScheme) && adminPassword == null) {
            throw new CommonException(
                    "%s must not be null for scheme '%s'",
                    PROPERTY_ADMIN_PASSWORD, SCHEME_DIGEST);
        }
        if (adminUser == null) {
            throw new CommonException(format, PROPERTY_ADMIN_USER);
        }
        if (kafkaScheme == null) {
            throw new CommonException(format, PROPERTY_KAFKA_SCHEME);
        }
        if (!SCHEME_SASL.equals(kafkaScheme)) {
            throw new CommonException(
                    "Scheme '%s' is wrong. Only '%s' is available for %s",
                    adminScheme, SCHEME_SASL, PROPERTY_KAFKA_SCHEME);
        }
        if (kafkaUser == null) {
            throw new CommonException(format, PROPERTY_KAFKA_USER);
        }
        if (keysRootPath == null) {
            throw new CommonException(format, PROPERTY_KEYS_ROOT_PATH);
        }
        PathUtils.validatePath(keysRootPath);
        if (keysRootPath.length() == 1) {
            throw new CommonException("Path must not be root");
        }
    }

    private static ZooKeeper getZooKeeperClient(
            String hostPort,
            String scheme,
            String user,
            String password)
            throws IOException {
        ZooKeeper zooKeeperClient = new ZooKeeper(hostPort, 3000, event -> {
        });
        if (SCHEME_DIGEST.equals(scheme)) {
            zooKeeperClient.addAuthInfo(scheme, String.format("%s:%s", user, password).getBytes());
        }
        LOGGER.info("Connection to the server {} is established", hostPort);
        return zooKeeperClient;
    }

    /**
     * Save the key to ZooKeeper and set ACLs for the key
     *
     * @param keyHolder key information
     * @throws CommonException if problem with ZooKeeper or with access list
     */
    public void saveKeyToZooKeeper(KeyHolder keyHolder) throws CommonException {
        try {
            String channel = keyHolder.getChannel();
            String field = keyHolder.getField();
            String name = keyHolder.getName();
            String resPath = keysRootPath;
            if (field != null && channel == null) {
                throw new CommonException("Channel must not be null for using with fields");
            }
            if (field != null) {
                resPath += getChannelPath(channel);
                checkChannel(resPath, EncryptionType.GRANULAR);
                String relativePath = getFieldPath(field);
                checkOrCreateFieldPath(resPath, relativePath);
                resPath += relativePath;
            } else if (channel != null) {
                resPath += getChannelPath(channel);
                checkChannel(resPath, EncryptionType.FULL);
            }
            resPath += getKeyName(keyHolder.getType(), name);
            createPathWithACL(resPath, keyHolder.getKey().toByteArray(), true);
            String expiredPath = resPath + getExpiredPath();
            if (keyHolder.getExpiredDate() != null) {
                createPathWithACL(expiredPath,
                        BigInteger.valueOf(keyHolder.getExpiredDate()).toByteArray(), false);
            } else {
                deletePathRecursive(expiredPath);
            }
            LOGGER.info("Key '{}' was created", resPath);
        } catch (KeeperException | InterruptedException | ClassNotFoundException e) {
            throw new CommonException(e);
        }
    }

    private void checkOrCreateFieldPath(String keysRootPath, String relativePath)
            throws KeeperException, InterruptedException {
        String[] paths = relativePath.substring(1).split(ZOOKEEPER_PATH_SEPARATOR);
        StringBuilder resPath = new StringBuilder(keysRootPath);
        for (String path : paths) {
            resPath.append(ZOOKEEPER_PATH_SEPARATOR).append(path);
            String resPathString = resPath.toString();
            checkOrCreateFieldPath(resPathString);
        }
    }

    private void checkOrCreateFieldPath(String resPathString)
            throws KeeperException, InterruptedException {
        if (zooKeeperClient.exists(resPathString, false) != null) {
            checkACL(keysRootPath);
        } else {
            createPathWithACL(resPathString, null, false);
        }
    }

    private void checkChannel(String path, EncryptionType type)
            throws KeeperException, InterruptedException, ClassNotFoundException {
        if (!checkChannel(path, type, null)) {
            throw new CommonException("Channel '%s' does not exist", path);
        }
    }

    private boolean checkChannel(String path, EncryptionType type, Class<?> clazz)
            throws KeeperException, InterruptedException, ClassNotFoundException {
        if (zooKeeperClient.exists(path, false) != null) {
            checkACL(path);
            byte[] data = zooKeeperClient.getData(path, false, new Stat());
            checkType(path, data, type);
            checkClass(path, data, clazz);
            return true;
        }
        return false;
    }

    private void checkType(String path, byte[] data, EncryptionType type)
            throws KeeperException, InterruptedException {
        if (type == null) {
            return;
        }
        EncryptionType current = getEncryptionType(data);
        if (type != current) {
            throw new CommonException("Changing the encryption type is not available. " +
                    "Path '%s', current type '%s', new type '%s'",
                    path, current, type);
        }
    }

    private void checkClass(String path, byte[] data, Class<?> clazz)
            throws KeeperException, InterruptedException, ClassNotFoundException {
        if (clazz == null) {
            return;
        }

        Class<?> current = getAccessorClass(data);
        if (clazz != current) {
            zooKeeperClient.setData(path, serialize(data[0], clazz), -1);
        }
    }

    private boolean checkOrCreateChannel(String path, EncryptionType type, Class<?> clazz)
            throws KeeperException, InterruptedException, ClassNotFoundException {
        if (checkChannel(path, type, clazz)) {
            return false;
        }
        if (type == null) {
            createPathWithACL(path, null, false);
        } else if (clazz == null) {
            createPathWithACL(path, new byte[]{type.getCode()}, false);
        } else {
            createPathWithACL(path, serialize(type, clazz), false);
        }
        return true;
    }

    private static byte[] serialize(EncryptionType type, Class<?> clazz) {
        return serialize(type.getCode(), clazz);
    }

    private static byte[] serialize(byte code, Class<?> clazz) {
        byte[] classNameBytes = clazz.getCanonicalName().getBytes();
        byte[] data = new byte[classNameBytes.length + 1];
        data[0] = code;
        System.arraycopy(classNameBytes, 0, data, 1, classNameBytes.length);
        return data;
    }

    private void makeInsecurePath(String keysRootPath)
            throws KeeperException, InterruptedException {
        String[] paths = keysRootPath.substring(1).split(ZOOKEEPER_PATH_SEPARATOR);
        StringBuilder resPath = new StringBuilder();
        for (int i = 0; i < paths.length - 1; i++) {
            resPath.append(ZOOKEEPER_PATH_SEPARATOR).append(paths[i]);
            String resPathString = resPath.toString();
            if (zooKeeperClient.exists(resPathString, false) == null) {
                createPath(resPathString);
            }
        }
    }

    private void createPath(String path)
            throws KeeperException, InterruptedException {
        zooKeeperClient.create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        LOGGER.info("Path {} created", path);
    }

    private void createPathWithACL(String path, byte[] data, boolean isKey)
            throws KeeperException, InterruptedException {
        deletePathRecursive(path);
        List<ACL> acls = getACLs(isKey);
        LOGGER.debug("ACL list {} for path {}", acls, path);
        zooKeeperClient.create(path, data, acls, CreateMode.PERSISTENT);
        LOGGER.info("Path {} with acls {} created ", path, acls);
    }

    private List<ACL> getACLs(boolean isKey) {
        ACL adminAcl = new ACL();
        String adminId = adminUser;
        if (SCHEME_DIGEST.equals(adminScheme)) {
            try {
                adminId += ":" + DigestAuthenticationProvider.generateDigest(adminPassword).split(":")[1];
            } catch (NoSuchAlgorithmException e) {
                //unreachable code
                throw new CommonException(e);
            }
        }
        adminAcl.setId(new Id(adminScheme, adminId));
        adminAcl.setPerms(ZooDefs.Perms.WRITE | ZooDefs.Perms.DELETE);
        if (!isKey) {
            adminAcl.setPerms(adminAcl.getPerms() | ZooDefs.Perms.READ | ZooDefs.Perms.CREATE);
        }
        ACL kafkaAcl = new ACL();
        kafkaAcl.setId(new Id(kafkaScheme, kafkaUser));
        kafkaAcl.setPerms(ZooDefs.Perms.READ);
        return Arrays.asList(adminAcl, kafkaAcl);
    }

    private void checkACL(String resPath)
            throws KeeperException, InterruptedException {
        List<ACL> acls = zooKeeperClient.getACL(resPath, null);
        List<ACL> correct = getACLs(false);
        String message = String.format(
                "Wrong ACL list %s for path %s. Should be %s", acls, resPath, correct);
        if (acls.size() != correct.size()) {
            throw new CommonException(message);
        }
        Optional<ACL> wrongACLs = correct.stream().filter(acl -> !acls.contains(acl)).findAny();
        if (wrongACLs.isPresent()) {
            throw new CommonException(message);
        }
    }

    /**
     * Delete the key from ZooKeeper
     *
     * @param channel channel which is used for the key. If null then key is
     *                searched for all channels
     * @param name    the key name
     * @param type    client type
     * @throws CommonException if key does not exists or problem with ZooKeeper
     */
    public void deleteKeyFromZooKeeper(String channel, String name, ClientType type)
            throws CommonException {
        deleteKeyFromZooKeeper(channel, name, type, null);
    }

    /**
     * Delete the key from ZooKeeper
     *
     * @param channel channel which is used for the key. If null then key is
     *                searched for all channels
     * @param name    the key name
     * @param type    client type
     * @param field   the field in the data structure to which this key belongs
     * @throws CommonException if key does not exists or problem with ZooKeeper
     */
    public void deleteKeyFromZooKeeper(String channel, String name, ClientType type, String field)
            throws CommonException {
        String resPath = keysRootPath;
        if (channel != null) {
            resPath += getChannelPath(channel);
        }
        if (field != null) {
            resPath += getFieldPath(field);
        }
        resPath += getKeyName(type, name);
        try {
            if (!deletePathRecursive(resPath)) {
                throw new CommonException(
                        "Key '%s' with type '%s' for channel '%s' with field '%s' does not exist",
                        name, type, channel, field);
            }
            deletePathRecursive(resPath + getExpiredPath());
        } catch (KeeperException | InterruptedException e) {
            throw new CommonException(e);
        }
        LOGGER.info("Key '{}' was deleted", resPath);
    }

    private boolean deletePathRecursive(String resPath) throws KeeperException, InterruptedException {
        if (zooKeeperClient.exists(resPath, false) != null) {
            if (isAccessible(resPath)) {
                List<String> children = zooKeeperClient.getChildren(resPath, false);
                for (String child : children) {
                    deletePathRecursive(resPath + ZOOKEEPER_PATH_SEPARATOR + child);
                }
            }
            zooKeeperClient.delete(resPath, -1);
            return true;
        }
        return false;
    }

    /**
     * Create the channel in ZooKeeper if not exists otherwise checks access list for channel
     *
     * @param channel channel name for creating
     * @throws CommonException if problem with ZooKeeper or with access list
     */
    public void createChannelInZooKeeper(String channel) throws CommonException {
        createChannelInZooKeeper(channel, EncryptionType.FULL, null);
    }

    /**
     * Create the channel in ZooKeeper if not exists otherwise checks access list for channel
     *
     * @param channel       channel name for creating
     * @param type          encryption type
     * @param accessorClass accessor class
     * @throws CommonException if problem with ZooKeeper or with access list
     */
    public void createChannelInZooKeeper(
            String channel,
            EncryptionType type,
            Class<? extends StructuredDataAccessor> accessorClass)
            throws CommonException {
        if (type != null && type == EncryptionType.GRANULAR && accessorClass == null) {
            throw new CommonException(
                    "Accessor class must not be null for channel with encryption type '%s'",
                    EncryptionType.GRANULAR);
        }
        if ((type == null || type != EncryptionType.GRANULAR) && accessorClass != null) {
            throw new CommonException(
                    "Accessor class is available only for channel with encryption type '%s'",
                    EncryptionType.GRANULAR);
        }
        checkAccessorClass(accessorClass);
        try {
            String resPath = keysRootPath + getChannelPath(channel);
            if (checkOrCreateChannel(resPath, type, accessorClass)) {
                LOGGER.info("Channel '{}' was created", resPath);
            } else {
                LOGGER.debug("Channel '{}' already exists", resPath);
            }
        } catch (KeeperException | InterruptedException | ClassNotFoundException e) {
            throw new CommonException(e);
        }
    }

    private void checkAccessorClass(Class<? extends StructuredDataAccessor> accessorClass) {
        if (accessorClass == null) {
            return;
        }
        if (Modifier.isInterface(accessorClass.getModifiers()) ||
                Modifier.isAbstract(accessorClass.getModifiers())) {
            throw new CommonException(
                    "Accessor class '%s' must be a class rather than an interface or abstract",
                    accessorClass);
        }
        Optional<Constructor<?>> defaultConstructor = Stream.of(accessorClass.getConstructors())
                .filter(c -> c.getParameterCount() == 0).findAny();
        if (!defaultConstructor.isPresent() ||
                !Modifier.isPublic(defaultConstructor.get().getModifiers())) {
            throw new CommonException(
                    "Accessor class '%s' must contain public default constructor",
                    accessorClass);
        }
    }

    /**
     * Delete the channel from ZooKeeper
     *
     * @param channel channel name for creating
     * @throws CommonException if channel does not exists or problem with ZooKeeper
     */
    public void deleteChannelFromZooKeeper(String channel) throws CommonException {
        String resPath = keysRootPath + getChannelPath(channel);
        try {
            if (!deletePathRecursive(resPath)) {
                throw new CommonException("Channel '%s' does not exist", channel);
            }
        } catch (KeeperException | InterruptedException e) {
            throw new CommonException(e);
        }
        LOGGER.info("Channel '{}' was deleted", resPath);
    }

    @Override
    public void close() throws CommonException {
        try {
            if (zooKeeperClient != null) {
                zooKeeperClient.close();
            }
        } catch (InterruptedException e) {
            throw new CommonException(e);
        }
    }

}
