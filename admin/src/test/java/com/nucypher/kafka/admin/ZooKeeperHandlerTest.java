package com.nucypher.kafka.admin;

import com.nucypher.kafka.clients.granular.StructuredDataAccessor;
import com.nucypher.kafka.clients.granular.StructuredDataAccessorStub;
import com.nucypher.kafka.errors.CommonException;
import com.nucypher.kafka.utils.WrapperReEncryptionKey;
import com.nucypher.kafka.zk.ClientType;
import com.nucypher.kafka.zk.DataUtils;
import com.nucypher.kafka.zk.EncryptionType;
import com.nucypher.kafka.zk.KeyHolder;
import com.nucypher.kafka.zk.ZooKeeperSASLResource;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.hamcrest.core.StringContains;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;


/**
 * Test for {@link AdminZooKeeperHandler}
 *
 * @author szotov
 */
public class ZooKeeperHandlerTest {

    private static final List<ACL> ACL_LIST_PATH = new ArrayList<>();
    private static final List<ACL> ACL_LIST_KEY = new ArrayList<>();

    static {
        ACL adminAclPath = new ACL();
        adminAclPath.setId(new Id("sasl", "zkAdmin1"));
        adminAclPath.setPerms(ZooDefs.Perms.WRITE | ZooDefs.Perms.DELETE |
                ZooDefs.Perms.READ | ZooDefs.Perms.CREATE);
        ACL adminAclKey = new ACL();
        adminAclKey.setId(new Id("sasl", "zkAdmin1"));
        adminAclKey.setPerms(ZooDefs.Perms.WRITE | ZooDefs.Perms.DELETE);
        ACL kafkaAcl = new ACL();
        kafkaAcl.setId(new Id("sasl", "kafka"));
        kafkaAcl.setPerms(ZooDefs.Perms.READ);
        ACL otherAdminAclPath = new ACL();
        otherAdminAclPath.setId(new Id("sasl", "zkAdmin2"));
        otherAdminAclPath.setPerms(ZooDefs.Perms.WRITE | ZooDefs.Perms.DELETE |
                ZooDefs.Perms.READ | ZooDefs.Perms.CREATE);

        ACL_LIST_PATH.add(adminAclPath);
        ACL_LIST_PATH.add(kafkaAcl);
        ACL_LIST_KEY.add(adminAclKey);
        ACL_LIST_KEY.add(kafkaAcl);
    }

    @ClassRule
    public static final ZooKeeperSASLResource ZOO_KEEPER_RESOURCE = new ZooKeeperSASLResource();
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private AdminZooKeeperHandler zooKeeperHandler;
    private CuratorFramework curator;
    private String rootPath;
    private Properties properties = new Properties();

    private WrapperReEncryptionKey simpleKey = DataUtils.getReEncryptionKeySimple();
    private WrapperReEncryptionKey complexKey = DataUtils.getReEncryptionKeyComplex();
    private WrapperReEncryptionKey emptyKey = DataUtils.getReEncryptionKeyEmpty();
    private byte[] simpleKeyBytes = simpleKey.toByteArray();
    private byte[] complexKeyBytes = complexKey.toByteArray();
    private byte[] emptyKeyBytes = emptyKey.toByteArray();
    private Long expired1 = DataUtils.getExpiredMillis();
    private Long expired2 = DataUtils.getExpiredMillis();
    private byte[] expiredBytes1 = DataUtils.getByteArrayFromExpired(expired1);
    private byte[] expiredBytes2 = DataUtils.getByteArrayFromExpired(expired2);

    /**
     * Initializing
     */
    @Before
    public void initialize() throws Exception {
        rootPath = "/keys/" + UUID.randomUUID().toString();
        properties.setProperty("zookeeper.server",
                ZOO_KEEPER_RESOURCE.getZooKeeperTestingServer().getConnectString());
        properties.setProperty("admin.scheme", "sasl");
        properties.setProperty("admin.user", "zkAdmin1");
        properties.setProperty("kafka.scheme", "sasl");
        properties.setProperty("kafka.user", "kafka");
        properties.setProperty("keys.path", rootPath);

        zooKeeperHandler = new AdminZooKeeperHandler(properties);
        curator = ZOO_KEEPER_RESOURCE.getAdminApacheCuratorFramework();

        curator.create().withACL(ACL_LIST_PATH)
                .forPath(rootPath + "/topic1-channel", null);
        curator.create().withACL(ACL_LIST_KEY)
                .forPath(rootPath + "/alice-producer", simpleKeyBytes);
        curator.create().withACL(ACL_LIST_PATH)
                .forPath(rootPath + "/topic2-channel", DataUtils.getFullEncryptedChannel());

        curator.create().withACL(ACL_LIST_PATH)
                .forPath(rootPath + "/topic-f-channel", DataUtils.getPartialEncryptedChannel());
        curator.create().withACL(ACL_LIST_PATH)
                .forPath(rootPath + "/topic-f-channel/a-field");
        curator.create().withACL(ACL_LIST_PATH)
                .forPath(rootPath + "/topic-f-channel/a-field/c-field");
        curator.create().withACL(ACL_LIST_PATH)
                .forPath(rootPath + "/topic-f-channel/b-field");

        curator.create().withACL(ACL_LIST_KEY).forPath(
                rootPath + "/topic-f-channel/a-field/c-field/alice-consumer", simpleKeyBytes);
        curator.create().withACL(ACL_LIST_PATH).forPath(
                rootPath + "/topic-f-channel/a-field/c-field/alice-consumer-expired", expiredBytes2);
    }

    /**
     * Test adding key without fields
     */
    @Test
    public void testAddKeyWithoutFields() throws Exception {
        KeyHolder key = new KeyHolder("topic1", "alice", ClientType.PRODUCER, simpleKey);
        zooKeeperHandler.saveKeyToZooKeeper(key);
        String path = rootPath + "/topic1-channel/alice-producer";
        assertNotNull(curator.checkExists().forPath(path));
        assertNull(curator.checkExists().forPath(getExpiredPath(path)));
        assertEquals(ACL_LIST_KEY, curator.getACL().forPath(path));
        assertArrayEquals(simpleKeyBytes, curator.getData().forPath(path));

        key = new KeyHolder("topic1", "alice", ClientType.PRODUCER, complexKey, expired1);
        zooKeeperHandler.saveKeyToZooKeeper(key);
        path = rootPath + "/topic1-channel/alice-producer";
        assertNotNull(curator.checkExists().forPath(path));
        assertNotNull(curator.checkExists().forPath(getExpiredPath(path)));
        assertArrayEquals(expiredBytes1, curator.getData().forPath(getExpiredPath(path)));
        assertEquals(ACL_LIST_KEY, curator.getACL().forPath(path));
        assertEquals(ACL_LIST_PATH, curator.getACL().forPath(getExpiredPath(path)));
        assertArrayEquals(complexKeyBytes, curator.getData().forPath(path));

        path = rootPath + "/topic2-channel/alice-producer";
        key = KeyHolder.builder().setChannel("topic2").setName("alice")
                .setType(ClientType.PRODUCER).setKey(complexKey).build();
        zooKeeperHandler.saveKeyToZooKeeper(key);
        assertNotNull(curator.checkExists().forPath(path));

        key = new KeyHolder(null, "alice", ClientType.PRODUCER, complexKey, expired2);
        zooKeeperHandler.saveKeyToZooKeeper(key);
        path = rootPath + "/alice-producer";
        assertNotNull(curator.checkExists().forPath(path));
        assertNotNull(curator.checkExists().forPath(getExpiredPath(path)));
        assertArrayEquals(expiredBytes2, curator.getData().forPath(getExpiredPath(path)));
        assertEquals(ACL_LIST_KEY, curator.getACL().forPath(path));
        assertEquals(ACL_LIST_PATH, curator.getACL().forPath(getExpiredPath(path)));
        assertArrayEquals(complexKeyBytes, curator.getData().forPath(path));
        key = KeyHolder.builder().setName("alice").setType(ClientType.PRODUCER).setKey(complexKey).build();
        zooKeeperHandler.saveKeyToZooKeeper(key);
        assertNull(curator.checkExists().forPath(getExpiredPath(path)));
    }

    private String getExpiredPath(String path) {
        return path + "-expired";
    }

    /**
     * Test adding key with fields
     */
    @Test
    public void testAddKeyWithFields() throws Exception {
        KeyHolder key = KeyHolder.builder().setChannel("topic-f").setName("alice")
                .setType(ClientType.PRODUCER).setKey(simpleKey).setField("a.c").build();
        zooKeeperHandler.saveKeyToZooKeeper(key);
        String path = rootPath + "/topic-f-channel/a-field/c-field/alice-producer";
        assertNotNull(curator.checkExists().forPath(path));
        assertNull(curator.checkExists().forPath(getExpiredPath(path)));
        assertEquals(ACL_LIST_KEY, curator.getACL().forPath(path));
        assertArrayEquals(simpleKeyBytes, curator.getData().forPath(path));

        key = KeyHolder.builder().setChannel("topic-f").setName("alice")
                .setType(ClientType.CONSUMER).setKey(complexKey)
                .setExpiredDate(expired1).setField("b").build();
        zooKeeperHandler.saveKeyToZooKeeper(key);
        path = rootPath + "/topic-f-channel/b-field/alice-consumer";
        assertNotNull(curator.checkExists().forPath(path));
        assertNotNull(curator.checkExists().forPath(getExpiredPath(path)));
        assertArrayEquals(expiredBytes1, curator.getData().forPath(getExpiredPath(path)));
        assertEquals(ACL_LIST_KEY, curator.getACL().forPath(path));
        assertEquals(ACL_LIST_PATH, curator.getACL().forPath(getExpiredPath(path)));
        assertArrayEquals(complexKeyBytes, curator.getData().forPath(path));

        key = KeyHolder.builder().setChannel("topic-f").setName("alice")
                .setType(ClientType.CONSUMER).setKey(emptyKey).setField("a.c").build();
        zooKeeperHandler.saveKeyToZooKeeper(key);
        path = rootPath + "/topic-f-channel/a-field/c-field/alice-consumer";
        assertNotNull(curator.checkExists().forPath(path));
        assertNull(curator.checkExists().forPath(getExpiredPath(path)));
        assertEquals(ACL_LIST_KEY, curator.getACL().forPath(path));
        assertNull(curator.getData().forPath(path));

        //new topic
        curator.create().withACL(ACL_LIST_PATH)
                .forPath(rootPath + "/topic-f2-channel", DataUtils.getPartialEncryptedChannel());
        key = KeyHolder.builder().setChannel("topic-f2").setName("alice")
                .setType(ClientType.CONSUMER).setKey(emptyKey).setField("a").build();
        zooKeeperHandler.saveKeyToZooKeeper(key);
        path = rootPath + "/topic-f2-channel/a-field";
        assertNotNull(curator.checkExists().forPath(path));
        assertEquals(ACL_LIST_PATH, curator.getACL().forPath(path));
        path = rootPath + "/topic-f2-channel/a-field/alice-consumer";
        assertNull(curator.checkExists().forPath(getExpiredPath(path)));
        assertEquals(ACL_LIST_KEY, curator.getACL().forPath(path));
        assertEquals(ACL_LIST_PATH, curator.getACL().forPath(rootPath));
        assertNull(curator.getData().forPath(path));

        key = KeyHolder.builder().setChannel("topic-f2").setName("alice")
                .setType(ClientType.CONSUMER).setKey(emptyKey).setField("b.c").build();
        zooKeeperHandler.saveKeyToZooKeeper(key);
        path = rootPath + "/topic-f2-channel/b-field";
        assertNotNull(curator.checkExists().forPath(path));
        assertEquals(ACL_LIST_PATH, curator.getACL().forPath(path));
        path = rootPath + "/topic-f2-channel/b-field/c-field";
        assertNotNull(curator.checkExists().forPath(path));
        assertEquals(ACL_LIST_PATH, curator.getACL().forPath(path));
        path = rootPath + "/topic-f2-channel/b-field/c-field/alice-consumer";
        assertNull(curator.checkExists().forPath(getExpiredPath(path)));
        assertEquals(ACL_LIST_KEY, curator.getACL().forPath(path));
        assertEquals(ACL_LIST_PATH, curator.getACL().forPath(rootPath));
        assertNull(curator.getData().forPath(path));

        //intermediate fields
        key = KeyHolder.builder().setChannel("topic-f").setName("alice")
                .setType(ClientType.CONSUMER).setKey(emptyKey).setField("a").build();
        zooKeeperHandler.saveKeyToZooKeeper(key);
        path = rootPath + "/topic-f-channel/a-field/alice-consumer";
        assertNotNull(curator.checkExists().forPath(path));

        key = KeyHolder.builder().setChannel("topic-f").setName("alice")
                .setType(ClientType.CONSUMER).setKey(emptyKey).setField("b.d").build();
        zooKeeperHandler.saveKeyToZooKeeper(key);
        path = rootPath + "/topic-f-channel/b-field/d-field";
        assertNotNull(curator.checkExists().forPath(path));
        path = rootPath + "/topic-f-channel/b-field/d-field/alice-consumer";
        assertNotNull(curator.checkExists().forPath(path));
    }

    /**
     * Test adding key to the insecure path
     */
    @Test
    public void testAddKeyToInsecurePath() throws Exception {
        curator.create().withACL(Ids.OPEN_ACL_UNSAFE).forPath(rootPath + "/topic4-channel");
        expectedException.expect(CommonException.class);
        expectedException.expectMessage(StringContains.containsString("Wrong ACL list"));
        KeyHolder key = new KeyHolder("topic4", "alice", ClientType.PRODUCER, null);
        zooKeeperHandler.saveKeyToZooKeeper(key);
    }

    /**
     * Test adding key with fields to the full encryption channel
     */
    @Test
    public void testAddKeyWithFieldsToFullChannel() throws Exception {
        expectedException.expect(CommonException.class);
        expectedException.expectMessage(StringContains.containsString(
                "Changing the encryption type is not available"));
        KeyHolder key = KeyHolder.builder().setChannel("topic1").setName("alice")
                .setType(ClientType.CONSUMER).setKey(emptyKey).setField("a").build();
        zooKeeperHandler.saveKeyToZooKeeper(key);
    }

    /**
     * Test adding key without fields to the partial encryption channel
     */
    @Test
    public void testAddKeyWithoutFieldsToPartialChannel() throws Exception {
        expectedException.expect(CommonException.class);
        expectedException.expectMessage(StringContains.containsString(
                "Changing the encryption type is not available"));
        KeyHolder key = KeyHolder.builder().setChannel("topic-f").setName("alice")
                .setType(ClientType.CONSUMER).setKey(emptyKey).build();
        zooKeeperHandler.saveKeyToZooKeeper(key);
    }

    /**
     * Test adding key to not existing channel
     */
    @Test
    public void testAddKeyToNotExistingChannel() throws Exception {
        expectedException.expect(CommonException.class);
        expectedException.expectMessage(StringContains.containsString("not exist"));
        KeyHolder key = KeyHolder.builder().setChannel("topic-f2").setName("alice")
                .setType(ClientType.CONSUMER).setKey(emptyKey).build();
        zooKeeperHandler.saveKeyToZooKeeper(key);
    }

    /**
     * Test deleting key
     */
    @Test
    public void testDeleteKey() throws Exception {
        curator.create().withACL(ACL_LIST_KEY)
                .forPath(rootPath + "/topic1-channel/alice-producer", complexKeyBytes);
        curator.create().withACL(ACL_LIST_PATH)
                .forPath(rootPath + "/topic1-channel/alice-producer-expired", expiredBytes1);
        curator.create().withACL(ACL_LIST_KEY)
                .forPath(rootPath + "/topic1-channel/alice-consumer", emptyKeyBytes);
        curator.create().withACL(ACL_LIST_PATH)
                .forPath(rootPath + "/topic1-channel/alice-consumer-expired", expiredBytes2);
        curator.create().withACL(ACL_LIST_PATH)
                .forPath(rootPath + "/topic-f-channel/b-field/alice-consumer", simpleKeyBytes);

        zooKeeperHandler.deleteKeyFromZooKeeper("topic1", "alice", ClientType.PRODUCER);
        String path = rootPath + "/topic1-channel/alice-producer";
        assertNull(curator.checkExists().forPath(path));
        assertNull(curator.checkExists().forPath(getExpiredPath(path)));

        zooKeeperHandler.deleteKeyFromZooKeeper(null, "alice", ClientType.PRODUCER);
        path = rootPath + "/alice-producer";
        assertNull(curator.checkExists().forPath(path));
        assertNull(curator.checkExists().forPath(getExpiredPath(path)));

        zooKeeperHandler.deleteKeyFromZooKeeper("topic1", "alice", ClientType.CONSUMER);
        path = rootPath + "/topic1-channel/alice-consumer";
        assertNull(curator.checkExists().forPath(path));
        assertNull(curator.checkExists().forPath(getExpiredPath(path)));

        zooKeeperHandler.deleteKeyFromZooKeeper("topic-f", "alice", ClientType.CONSUMER, "a.c");
        path = rootPath + "/topic1-channel/alice-consumer";
        assertNull(curator.checkExists().forPath(path));
        assertNull(curator.checkExists().forPath(getExpiredPath(path)));

        zooKeeperHandler.deleteKeyFromZooKeeper("topic-f", "alice", ClientType.CONSUMER, "b");
        path = rootPath + "/topic1-channel/alice-consumer";
        assertNull(curator.checkExists().forPath(path));
        assertNull(curator.checkExists().forPath(getExpiredPath(path)));
    }

    /**
     * Test deleting non existing key
     */
    @Test
    public void testDeleteKeyWithException1() {
        expectedException.expect(CommonException.class);
        expectedException.expectMessage(StringContains.containsString("does not exist"));
        zooKeeperHandler.deleteKeyFromZooKeeper("topic10", "alice", ClientType.PRODUCER);
    }

    /**
     * Test deleting non existing key
     */
    @Test
    public void testDeleteKeyWithException2() {
        expectedException.expect(CommonException.class);
        expectedException.expectMessage(StringContains.containsString("does not exist"));
        zooKeeperHandler.deleteKeyFromZooKeeper("topic-f", "alice", ClientType.PRODUCER, "d");
    }

    public static class AnotherStub extends StructuredDataAccessorStub {

    }

    /**
     * Test creating channel
     */
    @Test
    public void testCreateChannel() throws Exception {
        zooKeeperHandler.createChannelInZooKeeper("topic4");
        String path = rootPath + "/topic4-channel";
        assertNotNull(curator.checkExists().forPath(path));
        assertArrayEquals(DataUtils.getFullEncryptedChannel(), curator.getData().forPath(path));

        zooKeeperHandler.createChannelInZooKeeper("topic5", EncryptionType.FULL, null);
        path = rootPath + "/topic5-channel";
        assertNotNull(curator.checkExists().forPath(path));
        assertArrayEquals(DataUtils.getFullEncryptedChannel(), curator.getData().forPath(path));

        zooKeeperHandler.createChannelInZooKeeper(
                "topic6", EncryptionType.GRANULAR, StructuredDataAccessorStub.class);
        path = rootPath + "/topic6-channel";
        assertNotNull(curator.checkExists().forPath(path));
        assertArrayEquals(DataUtils.getPartialEncryptedChannel(), curator.getData().forPath(path));

        zooKeeperHandler.createChannelInZooKeeper("topic2", EncryptionType.FULL, null);
        zooKeeperHandler.createChannelInZooKeeper(
                "topic6", EncryptionType.GRANULAR, StructuredDataAccessorStub.class);
    }

    /**
     * Test creating channel with different accessor class
     */
    @Test
    public void testCreateChannelWithDifferentClass() throws Exception {
        zooKeeperHandler.createChannelInZooKeeper(
                "topic-f", EncryptionType.GRANULAR, AnotherStub.class);
        String path = rootPath + "/topic-f-channel";
        byte[] data = curator.getData().forPath(path);
        String className = new String(Arrays.copyOfRange(data, 1, data.length));
        assertEquals(AnotherStub.class.getCanonicalName(), className);
    }

    /**
     * Test creating channel to the insecure path
     */
    @Test
    public void testCreateChannelToInsecurePath() throws Exception {
        curator.create().withACL(Ids.OPEN_ACL_UNSAFE).forPath(rootPath + "/topic4-channel");
        expectedException.expect(CommonException.class);
        expectedException.expectMessage(StringContains.containsString("Wrong ACL list"));
        zooKeeperHandler.createChannelInZooKeeper("topic4");
    }

    /**
     * Test creating existing channel with different encryption type
     */
    @Test
    public void testCreateChannelWithWrongType() throws Exception {
        expectedException.expect(CommonException.class);
        expectedException.expectMessage(StringContains.containsString(
                "Changing the encryption type is not available"));
        zooKeeperHandler.createChannelInZooKeeper(
                "topic2", EncryptionType.GRANULAR, StructuredDataAccessorStub.class);
    }

    /**
     * Test creating existing channel with wrong accessor class
     */
    @Test
    public void testCreateChannelWithWrongClass1() throws Exception {
        expectedException.expect(CommonException.class);
        expectedException.expectMessage(StringContains.containsString(
                "must be a class rather than an interface"));
        zooKeeperHandler.createChannelInZooKeeper(
                "topic-f", EncryptionType.GRANULAR, StructuredDataAccessor.class);
    }

    /**
     * Test creating existing channel with wrong accessor class
     */
    @Test
    public void testCreateChannelWithWrongClass2() throws Exception {
        expectedException.expect(CommonException.class);
        expectedException.expectMessage(StringContains.containsString(
                "must contain public default constructor"));
        zooKeeperHandler.createChannelInZooKeeper(
                "topic-f",
                EncryptionType.GRANULAR,
                new StructuredDataAccessorStub(){}.getClass());
    }

    /**
     * Test deleting channel
     */
    @Test
    public void testDeleteChannel() throws Exception {
        zooKeeperHandler.deleteChannelFromZooKeeper("topic1");
        assertNull(curator.checkExists().forPath(rootPath + "/topic1-channel"));
        zooKeeperHandler.deleteChannelFromZooKeeper("topic-f");
        assertNull(curator.checkExists().forPath(rootPath + "/topic-f-channel"));
    }

    /**
     * Test deleting non existing channel
     */
    @Test
    public void testDeleteChannelWithException() {
        expectedException.expect(CommonException.class);
        expectedException.expectMessage(StringContains.containsString("does not exist"));
        zooKeeperHandler.deleteChannelFromZooKeeper("topic10");
    }

    /**
     * Test creating new connection
     */
    @Test
    public void testNewConnection() throws Exception {
        int size = zooKeeperHandler.listAllKeys().size();
        AdminZooKeeperHandler newZooKeeperHandler = new AdminZooKeeperHandler(properties);
        assertEquals(size, newZooKeeperHandler.listAllKeys().size());

        curator.delete().deletingChildrenIfNeeded().forPath("/keys");
        new AdminZooKeeperHandler(properties);
        assertNotNull(curator.checkExists().forPath(rootPath));
    }

    /**
     * Close resources
     */
    @After
    public void close() {
        if (zooKeeperHandler != null) {
            zooKeeperHandler.close();
        }
    }

}
