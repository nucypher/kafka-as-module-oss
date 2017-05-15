package com.nucypher.kafka.zk;

import com.nucypher.kafka.errors.CommonException;
import com.nucypher.kafka.utils.WrapperReEncryptionKey;
import com.nucypher.kafka.clients.granular.StructuredDataAccessorStub;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooDefs.Perms;
import org.apache.zookeeper.data.ACL;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


/**
 * Test for {@link BaseZooKeeperHandler}
 *
 * @author szotov
 */
public class BaseZooKeeperHandlerTest {

    private static final List<ACL> ACLS = Collections.singletonList(new ACL(Perms.ADMIN, Ids.ANYONE_ID_UNSAFE));

    @ClassRule
    public static final ZooKeeperSASLResource ZOO_KEEPER_RESOURCE = new ZooKeeperSASLResource();
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private BaseZooKeeperHandler zooKeeperHandler;
    private CuratorFramework curator;
    private String rootPath;

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
        rootPath = "/" + UUID.randomUUID().toString();
        curator = ZOO_KEEPER_RESOURCE.getAdminApacheCuratorFramework();
        zooKeeperHandler = new BaseZooKeeperHandler(ZOO_KEEPER_RESOURCE.getApacheCuratorFramework()
                .getZookeeperClient().getZooKeeper(), rootPath);

        String zkAdminPath = rootPath + "/zkAdmin1";
        String topic1Path = zkAdminPath + "/topic1-channel";
        curator.create().creatingParentsIfNeeded().forPath(zkAdminPath);
        curator.create().forPath(topic1Path, DataUtils.getFullEncryptedChannel());
        curator.create().forPath(zkAdminPath + "/topic2-channel", null);
        curator.create().withACL(ACLS)
                .forPath(zkAdminPath + "/alice-producer", simpleKeyBytes);
        curator.create().withACL(ACLS)
                .forPath(topic1Path + "/alice-producer", complexKeyBytes);
        curator.create().forPath(topic1Path + "/alice-producer-expired",
                expiredBytes1);
        curator.create().withACL(ACLS)
                .forPath(topic1Path + "/alice-consumer", simpleKeyBytes);
        curator.create().forPath(topic1Path + "/alice-consumer-expired",
                expiredBytes2);
        curator.create().withACL(ACLS)
                .forPath(zkAdminPath + "/topic2-channel/alice2-consumer", complexKeyBytes);

        curator.create().withACL(ACLS).forPath(rootPath + "/zkAdmin2");
        curator.create().withACL(ACLS)
                .forPath(rootPath + "/zkAdmin2/alice3-consumer", simpleKeyBytes);

        curator.create().forPath(rootPath + "/zkAdmin3");
        curator.create().forPath(rootPath + "/zkAdmin3/topic-3-channel", null);
        curator.create().withACL(ACLS)
                .forPath(rootPath + "/zkAdmin3/topic-3-channel/alice-2-consumer", emptyKeyBytes);
        curator.create().forPath(rootPath + "/zkAdmin3/topic-3-channel/alice-2-consumer-expired",
                expiredBytes1);
        curator.create().forPath(topic1Path + "/alice-consumer1-expired",
                expiredBytes2);
        curator.create().forPath(zkAdminPath + "/topic1-channel/alice-consumer-expired1",
                expiredBytes2);
        curator.create().forPath(zkAdminPath + "/topic2-channel1");
        curator.create().forPath(zkAdminPath + "/asddfsf");
        curator.create().forPath(zkAdminPath + "/alice2-consumer1");
        curator.create().creatingParentsIfNeeded().forPath(topic1Path + "/alice-consumer1");

        String topicFPath = zkAdminPath + "/topic-f-channel";
        String aField = topicFPath + "/a-field";
        curator.create().forPath(topicFPath, DataUtils.getPartialEncryptedChannel());
        curator.create().forPath(aField);
        curator.create().forPath(topicFPath + "/b-field");
        curator.create().forPath(aField + "/c-field");
        curator.create().withACL(ACLS)
                .forPath(aField + "/c-field/alice-producer", simpleKeyBytes);
        curator.create().withACL(ACLS)
                .forPath(topicFPath + "/b-field/alice-consumer", simpleKeyBytes);
    }

    /**
     * Test listing keys
     */
    @Test
    public void testListKeys() throws Exception {
        Set<KeyHolder> keys = zooKeeperHandler.listAllKeys();
        assertEquals(7, keys.size());
        Set<KeyHolder> expected = new HashSet<>();
        expected.add(new KeyHolder(null, "alice", ClientType.PRODUCER, null));
        expected.add(new KeyHolder("topic1", "alice", ClientType.PRODUCER, null, expired1));
        expected.add(new KeyHolder("topic1", "alice", ClientType.CONSUMER, null, expired2));
        expected.add(new KeyHolder("topic-3", "alice-2", ClientType.CONSUMER, null, expired1));
        expected.add(KeyHolder.builder().setChannel("topic-f").setName("alice")
                .setType(ClientType.PRODUCER).setField("a.c").build()); //TODO think about separator
        expected.add(KeyHolder.builder().setChannel("topic-f").setName("alice")
                .setType(ClientType.CONSUMER).setField("b").build());
        KeyHolder key = new KeyHolder("topic2", "alice2", ClientType.CONSUMER, null);
        expected.add(key);
        assertEquals(expected, keys);

        curator.create().withACL(ACLS)
                .forPath(rootPath + "/zkAdmin1/topic2-channel/alice3-consumer", complexKeyBytes);
        curator.create().forPath(rootPath + "/zkAdmin1/topic2-channel/alice3-consumer-expired",
                expiredBytes2);
        keys = zooKeeperHandler.listAllKeys();
        assertEquals(8, keys.size());
        expected.add(new KeyHolder("topic2", "alice3", ClientType.CONSUMER, null, expired2));
        assertEquals(expected, keys);

        curator.delete().forPath(rootPath + "/zkAdmin1/topic2-channel/alice2-consumer");
        keys = zooKeeperHandler.listAllKeys();
        assertEquals(7, keys.size());
        expected.remove(key);
        assertEquals(expected, keys);
    }

    /**
     * Test getting key
     */
    @Test
    public void testGetKey() throws Exception {
        curator.setACL().withACL(Ids.OPEN_ACL_UNSAFE)
                .forPath(rootPath + "/zkAdmin1/topic1-channel/alice-producer");
        KeyHolder key = zooKeeperHandler.getKey("topic1", "alice", ClientType.PRODUCER);
        KeyHolder expected = new KeyHolder("topic1", "alice", ClientType.PRODUCER, complexKey, expired1);
        assertEquals(expected, key);

        curator.setACL().withACL(Ids.OPEN_ACL_UNSAFE)
                .forPath(rootPath + "/zkAdmin1/alice-producer");
        key = zooKeeperHandler.getKey(null, "alice", ClientType.PRODUCER);
        expected = new KeyHolder(null, "alice", ClientType.PRODUCER, simpleKey);
        assertEquals(expected, key);

        curator.create().forPath(rootPath + "/zkAdmin1/alice-producer-expired",
                DataUtils.getByteArrayFromExpired(0L));
        key = zooKeeperHandler.getKey(null, "alice", ClientType.PRODUCER);
//		assertNull(key);
        assertNotNull(key);

        curator.setACL().withACL(Ids.OPEN_ACL_UNSAFE)
                .forPath(rootPath + "/zkAdmin3/topic-3-channel/alice-2-consumer");
        key = zooKeeperHandler.getKey("topic-3", "alice-2", ClientType.CONSUMER);
        expected = new KeyHolder("topic-3", "alice-2", ClientType.CONSUMER, emptyKey, expired1);
        assertEquals(expected, key);

        key = zooKeeperHandler.getKey(null, "aliceeee", ClientType.PRODUCER);
        assertNull(key);
    }

    /**
     * Test getting key using field
     */
    @Test
    public void testGetKeyUsingField() throws Exception {
        curator.setACL().withACL(Ids.OPEN_ACL_UNSAFE)
                .forPath(rootPath + "/zkAdmin1/topic-f-channel/b-field/alice-consumer");
        KeyHolder key = zooKeeperHandler.getKey("topic-f", "alice", ClientType.CONSUMER, "b");
        KeyHolder expected = KeyHolder.builder().setChannel("topic-f").setName("alice")
                .setType(ClientType.CONSUMER).setField("b").setKey(simpleKey).build();
        assertEquals(expected, key);

        curator.setACL().withACL(Ids.OPEN_ACL_UNSAFE)
                .forPath(rootPath + "/zkAdmin1/topic-f-channel/a-field/c-field/alice-producer");
        key = zooKeeperHandler.getKey("topic-f", "alice", ClientType.PRODUCER, "a.c");
        expected = KeyHolder.builder().setChannel("topic-f").setName("alice")
                .setType(ClientType.PRODUCER).setField("a.c").setKey(simpleKey).build();
        assertEquals(expected, key);

        key = zooKeeperHandler.getKey("topic-f", "alice", ClientType.PRODUCER, "a");
        assertNull(key);
        key = zooKeeperHandler.getKey("topic-f", "alice", ClientType.PRODUCER);
        assertNull(key);
    }

    /**
     * Test checking if a key exists
     */
    @Test
    public void testKeyExists() throws Exception {
        assertTrue(zooKeeperHandler.isKeyExists(
                "topic1", "alice", ClientType.PRODUCER));
        assertTrue(zooKeeperHandler.isKeyExists(
                null, "alice", ClientType.PRODUCER));

        assertTrue(zooKeeperHandler.isKeyExists(
                "topic-3", "alice-2", ClientType.CONSUMER));
        assertFalse(zooKeeperHandler.isKeyExists(
                null, "aliceeee", ClientType.PRODUCER));

        assertTrue(zooKeeperHandler.isKeyExists(
                "topic-f", "alice", ClientType.CONSUMER, "b"));
        assertTrue(zooKeeperHandler.isKeyExists(
                "topic-f", "alice", ClientType.PRODUCER, "a.c"));
        assertFalse(zooKeeperHandler.isKeyExists(
                "topic-f", "alice", ClientType.PRODUCER, "a"));
        assertFalse(zooKeeperHandler.isKeyExists(
                "topic-f", "alice", ClientType.PRODUCER));
    }

    /**
     * Test listing channels
     */
    @Test
    public void testListChannels() throws Exception {
        Set<Channel> channels = zooKeeperHandler.listAllChannels();
        assertEquals(4, channels.size());
        Set<Channel> expected = new HashSet<>();
        expected.add(new Channel("topic1"));
        expected.add(new Channel("topic2"));
        expected.add(new Channel("topic-3"));
        Set<String> fields = new HashSet<>();
        fields.add("a");
        fields.add("a.c");
        fields.add("b");
        expected.add(new Channel("topic-f", fields, StructuredDataAccessorStub.class));
        assertEquals(expected, channels);

        curator.create().forPath(rootPath + "/zkAdmin1/topic4-channel", null);
        channels = zooKeeperHandler.listAllChannels();
        assertEquals(5, channels.size());
        expected.add(new Channel("topic4"));
        assertEquals(expected, channels);

        curator.delete().deletingChildrenIfNeeded().forPath(rootPath + "/zkAdmin1/topic2-channel");
        channels = zooKeeperHandler.listAllChannels();
        assertEquals(4, channels.size());
        expected.remove(new Channel("topic2"));
        assertEquals(expected, channels);
    }

    /**
     * Test checking channel existence
     */
    @Test
    public void testIsChannelExists() {
        assertTrue(zooKeeperHandler.isChannelExists("topic1"));
        assertFalse(zooKeeperHandler.isChannelExists("topic5"));
    }

    /**
     * Test getting channel data
     */
    @Test
    public void testGetChannel() throws Exception {
        assertEquals(new Channel("topic1"), zooKeeperHandler.getChannel("topic1"));
        Set<String> fields = new HashSet<>();
        fields.add("a");
        fields.add("a.c");
        fields.add("b");
        assertEquals(new Channel("topic-f", fields, StructuredDataAccessorStub.class),
                zooKeeperHandler.getChannel("topic-f"));
        assertNull(zooKeeperHandler.getChannel("topic0"));

        zooKeeperHandler = new BaseZooKeeperHandler(ZOO_KEEPER_RESOURCE.getApacheCuratorFramework()
                .getZookeeperClient().getZooKeeper(), "/keys0");
        assertNull(zooKeeperHandler.getChannel("topic1"));
    }

    /**
     * Test getting channel with wrong accessor class
     */
    @Test
    public void testGetCorruptedChannel() throws Exception {
        byte[] corrupted = new byte[2];
        corrupted[0] = EncryptionType.GRANULAR.getCode();
        corrupted[1] = 1;
        curator.create().forPath(rootPath + "/topic-f-error-channel", corrupted);
        expectedException.expect(CommonException.class);
        expectedException.expectCause(
                IsInstanceOf.<Throwable>instanceOf(ClassNotFoundException.class));
        zooKeeperHandler.getChannel("topic-f-error");
    }

    /**
     * Test getting channel with wrong encryption type
     */
    @Test
    public void testGetCorruptedChannel2() throws Exception {
        byte[] corrupted = new byte[1];
        corrupted[0] = -1;
        curator.create().forPath(rootPath + "/topic-f-error-channel", corrupted);
        expectedException.expect(CommonException.class);
        expectedException.expectMessage("No such encryption type with code '-1'");
        zooKeeperHandler.getChannel("topic-f-error");
    }
}
