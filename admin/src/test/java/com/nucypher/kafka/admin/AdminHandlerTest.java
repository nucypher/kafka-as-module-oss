package com.nucypher.kafka.admin;

import com.nucypher.kafka.TestConstants;
import com.nucypher.kafka.clients.granular.DataFormat;
import com.nucypher.kafka.clients.granular.JsonDataAccessor;
import com.nucypher.kafka.clients.granular.StructuredDataAccessorStub;
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
import org.bouncycastle.jce.ECNamedCurveTable;
import org.hamcrest.core.IsInstanceOf;
import org.hamcrest.core.StringContains;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.math.BigInteger;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.time.ZonedDateTime;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.verifyStatic;

/**
 * Test for {@link AdminHandler}
 *
 * @author szotov
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({KeyUtils.class, GranularUtils.class})
@PowerMockIgnore("javax.management.*")
public class AdminHandlerTest {

    private static final EncryptionAlgorithm ALGORITHM = TestConstants.ENCRYPTION_ALGORITHM;

    @Mock
    private AdminZooKeeperHandler zooKeeperHandler;
    @Mock
    private PrivateKey inputPrivateKey;
    @Mock
    private PrivateKey privateKey;
    @Mock
    private PublicKey publicKey;
    @Captor
    private ArgumentCaptor<String> stringCaptor;
    @Captor
    private ArgumentCaptor<KeyHolder> keyHolderCaptor;
    @Captor
    private ArgumentCaptor<KeyType> keyTypeCaptor;
    @Captor
    private ArgumentCaptor<KeyPair> keyPairCaptor;
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private AdminHandler handler;

    private String masterKey = "masterKey";
    private String clientKey = "clientKey";
    private String clientName = "name";
    private String channel = "channel";
    private String curve = "curve";
    private Set<String> fields = new HashSet<>();

    {
        fields.add("a.c");
        fields.add("b");
    }

    private WrapperReEncryptionKey key = new WrapperReEncryptionKey(ALGORITHM,
            BigInteger.ONE, ECNamedCurveTable.getParameterSpec("secp521r1"));

    /**
     * Initializing
     */
    @Before
    public void initialize() throws Exception {
        initMocks(this);
        mockStatic(KeyUtils.class, GranularUtils.class);
        when(KeyUtils.generateReEncryptionKey(
                any(), anyString(), anyString(), any(), anyString())).thenReturn(key);
        when(KeyUtils.generateReEncryptionKey(
                any(), (KeyPair) any(), any(), any(), anyString())).thenReturn(key);
        when(KeyUtils.getECKeyPairFromPEM(anyString()))
                .thenReturn(new KeyPair(null, inputPrivateKey));
        when(GranularUtils.deriveKeyFromData(anyString(), any())).thenReturn(privateKey);
        when(GranularUtils.getChannelFieldName(anyString(), anyString()))
                .then(invocation -> {
                    Object[] args = invocation.getArguments();
                    return args[0] + "-" + args[1];
                });

        handler = new AdminHandler(zooKeeperHandler);
    }

    /**
     * Test adding consumer re-encryption key
     */
    @Test
    public void testAddConsumerReKey() throws Exception {
        String curve = "curve";
        handler.generateAndSaveReEncryptionKey(
                ALGORITHM,
                masterKey,
                clientKey,
                curve,
                ClientType.CONSUMER,
                null,
                clientName,
                channel,
                null,
                null,
                null,
                null,
                null);

        verifyStatic(times(1));
        KeyUtils.generateReEncryptionKey(eq(ALGORITHM), stringCaptor.capture(),
                stringCaptor.capture(), keyTypeCaptor.capture(), stringCaptor.capture());
        List<String> values = stringCaptor.getAllValues();
        assertEquals(masterKey, values.get(0));
        assertEquals(clientKey, values.get(1));
        assertEquals(curve, values.get(2));
        assertEquals(KeyType.DEFAULT, keyTypeCaptor.getValue());

        verify(zooKeeperHandler, times(1)).saveKeyToZooKeeper(
                new KeyHolder(channel, clientName, ClientType.CONSUMER, key));
        verify(zooKeeperHandler, times(1))
                .createChannelInZooKeeper(channel, EncryptionType.FULL, null);
    }

    /**
     * Test adding consumer re-encryption key with fields
     */
    @Test
    public void testAddConsumerReKeyWithFields() throws Exception {
        handler.generateAndSaveReEncryptionKey(
                ALGORITHM,
                masterKey,
                clientKey,
                curve,
                ClientType.CONSUMER,
                null,
                clientName,
                channel,
                null,
                null,
                fields,
                null,
                null);

        verifyStatic(times(1));
        GranularUtils.deriveKeyFromData(eq(masterKey), eq(channel + "-a.c"));
        verifyStatic(times(1));
        GranularUtils.deriveKeyFromData(eq(masterKey), eq(channel + "-b"));
        verifyStatic(times(1));
        KeyUtils.getECKeyPairFromPEM(eq(clientKey));
        verifyStatic(times(2));
        KeyUtils.generateReEncryptionKey(eq(ALGORITHM),
                keyPairCaptor.capture(), keyPairCaptor.capture(), eq(KeyType.DEFAULT), eq(curve));
        List<KeyPair> values = keyPairCaptor.getAllValues();
        assertEquals(privateKey, values.get(0).getPrivate());
        assertEquals(inputPrivateKey, values.get(1).getPrivate());
        assertEquals(privateKey, values.get(2).getPrivate());
        assertEquals(inputPrivateKey, values.get(3).getPrivate());

        verify(zooKeeperHandler, times(1)).saveKeyToZooKeeper(
                KeyHolder.builder().setChannel(channel).setName(clientName)
                        .setType(ClientType.CONSUMER).setKey(key).setField("a.c").build());
        verify(zooKeeperHandler, times(1)).saveKeyToZooKeeper(
                KeyHolder.builder().setChannel(channel).setName(clientName)
                        .setType(ClientType.CONSUMER).setKey(key).setField("b").build());
        verify(zooKeeperHandler, never())
                .createChannelInZooKeeper(channel, EncryptionType.GRANULAR, null);
    }

    /**
     * Test adding producer re-encryption key with expired date
     */
    @Test
    public void testAddProducerReKey() throws Exception {
        ZonedDateTime dateTime = ZonedDateTime.now();

        handler.generateAndSaveReEncryptionKey(
                ALGORITHM,
                masterKey,
                clientKey,
                null,
                ClientType.PRODUCER,
                null,
                clientName,
                channel,
                null,
                dateTime,
                null,
                null,
                null);

        verifyStatic(times(1));
        KeyUtils.generateReEncryptionKey(eq(ALGORITHM), stringCaptor.capture(),
                stringCaptor.capture(), keyTypeCaptor.capture(), stringCaptor.capture());
        List<String> values = stringCaptor.getAllValues();
        assertEquals(clientKey, values.get(0));
        assertEquals(masterKey, values.get(1));
        assertNull(values.get(2));
        assertEquals(KeyType.PRIVATE, keyTypeCaptor.getValue());

        verify(zooKeeperHandler, times(1))
                .createChannelInZooKeeper(channel, EncryptionType.FULL, null);
        verify(zooKeeperHandler, times(1)).saveKeyToZooKeeper(
                new KeyHolder(channel, clientName, ClientType.PRODUCER,
                        key, dateTime.toInstant().toEpochMilli()));
    }

    /**
     * Test adding producer re-encryption key with expired date and fields
     */
    @Test
    public void testAddProducerReKeyWithFields() throws Exception {
        ZonedDateTime dateTime = ZonedDateTime.now();

        handler.generateAndSaveReEncryptionKey(
                ALGORITHM,
                masterKey,
                clientKey,
                null,
                ClientType.PRODUCER,
                null,
                clientName,
                channel,
                null,
                dateTime,
                fields,
                StructuredDataAccessorStub.class.getCanonicalName(),
                null);

        verifyStatic(times(2));
        KeyUtils.generateReEncryptionKey(eq(ALGORITHM), keyPairCaptor.capture(),
                keyPairCaptor.capture(), eq(KeyType.PRIVATE), (String) isNull());
        List<KeyPair> values = keyPairCaptor.getAllValues();
        assertEquals(inputPrivateKey, values.get(0).getPrivate());
        assertEquals(privateKey, values.get(1).getPrivate());
        assertEquals(inputPrivateKey, values.get(2).getPrivate());
        assertEquals(privateKey, values.get(3).getPrivate());

        verify(zooKeeperHandler, times(1)).saveKeyToZooKeeper(
                KeyHolder.builder().setChannel(channel).setName(clientName)
                        .setType(ClientType.PRODUCER).setKey(key)
                        .setExpiredDate(dateTime.toInstant().toEpochMilli()).setField("a.c").build());
        verify(zooKeeperHandler, times(1)).saveKeyToZooKeeper(
                KeyHolder.builder().setChannel(channel).setName(clientName)
                        .setType(ClientType.PRODUCER).setKey(key)
                        .setExpiredDate(dateTime.toInstant().toEpochMilli()).setField("b").build());
        verify(zooKeeperHandler, times(1))
                .createChannelInZooKeeper(
                        channel, EncryptionType.GRANULAR, StructuredDataAccessorStub.class);
    }

    /**
     * Test adding re-encryption key with fields and data format
     */
    @Test
    public void testAddReKeyWithFieldsAndFormat() throws Exception {
        handler.generateAndSaveReEncryptionKey(
                ALGORITHM,
                masterKey,
                clientKey,
                null,
                ClientType.PRODUCER,
                null,
                clientName,
                channel,
                null,
                null,
                fields,
                null,
                DataFormat.JSON);

        verify(zooKeeperHandler, times(1)).saveKeyToZooKeeper(
                KeyHolder.builder().setChannel(channel).setName(clientName)
                        .setType(ClientType.PRODUCER).setKey(key)
                        .setField("a.c").build());
        verify(zooKeeperHandler, times(1)).saveKeyToZooKeeper(
                KeyHolder.builder().setChannel(channel).setName(clientName)
                        .setType(ClientType.PRODUCER).setKey(key)
                        .setField("b").build());
        verify(zooKeeperHandler, times(1))
                .createChannelInZooKeeper(channel, EncryptionType.GRANULAR, JsonDataAccessor.class);
    }

    /**
     * Test adding producer re-encryption key with lifetime in days
     */
    @Test
    public void testAddProducerReKeyWithDays() throws Exception {
        int days = 10;
        long minDate = System.currentTimeMillis() + TimeUnit.DAYS.toMillis(days);

        handler.generateAndSaveReEncryptionKey(
                ALGORITHM,
                masterKey,
                clientKey,
                null,
                ClientType.PRODUCER,
                null,
                clientName,
                channel,
                days,
                null,
                null,
                null,
                null);

        verify(zooKeeperHandler, times(1))
                .saveKeyToZooKeeper(keyHolderCaptor.capture());
        KeyHolder keyHolder = keyHolderCaptor.getValue();
        assertEquals(channel, keyHolder.getChannel());
        assertEquals(clientName, keyHolder.getName());
        assertEquals(ClientType.PRODUCER, keyHolder.getType());
        assertSame(key, keyHolder.getKey());
        assertTrue(Math.abs(keyHolder.getExpiredDate() - minDate) <
                TimeUnit.SECONDS.toMillis(1));
    }

    /**
     * Test deleting re-encryption key
     */
    @Test
    public void testDeleteReKey() {
        handler.deleteReEncryptionKey(ClientType.PRODUCER, clientName, channel, null);
        verify(zooKeeperHandler, times(1))
                .deleteKeyFromZooKeeper(channel, clientName, ClientType.PRODUCER, null);
        handler.deleteReEncryptionKey(ClientType.CONSUMER, clientName, channel, null);
        verify(zooKeeperHandler, times(1))
                .deleteKeyFromZooKeeper(channel, clientName, ClientType.CONSUMER, null);
        handler.deleteReEncryptionKey(ClientType.CONSUMER, clientName, channel, "a.c");
        verify(zooKeeperHandler, times(1))
                .deleteKeyFromZooKeeper(channel, clientName, ClientType.CONSUMER, "a.c");
    }

    /**
     * Test deleting all re-encryption subkeys
     */
    @Test
    public void testDeleteSubkeys() {
        when(zooKeeperHandler.getChannel(channel))
                .thenReturn(new Channel(channel, fields, null));
        when(zooKeeperHandler
                .isKeyExists(channel, clientName, ClientType.CONSUMER, "a.c"))
                .thenReturn(true);
        handler.deleteReEncryptionKey(ClientType.CONSUMER, clientName, channel, null);
        verify(zooKeeperHandler, times(1))
                .deleteKeyFromZooKeeper(channel, clientName, ClientType.CONSUMER, "a.c");
        verify(zooKeeperHandler, times(0))
                .deleteKeyFromZooKeeper(channel, clientName, ClientType.CONSUMER, "b");
    }

    /**
     * Test listing re-encryption keys and channels
     */
    @Test
    public void testList() {
        handler.listAll(false);
        verify(zooKeeperHandler, times(1)).listAllKeys();
        handler.listAll(true);
        verify(zooKeeperHandler, times(1)).listAllChannels();
    }

    /**
     * Test deleting channel
     */
    @Test
    public void testDeleteChannel() {
        handler.deleteChannel(channel);
        verify(zooKeeperHandler, times(1))
                .deleteChannelFromZooKeeper(channel);
    }

    /**
     * Test creating channel
     */
    @Test
    public void testCreateChannel() {
        handler.createChannel(channel, EncryptionType.FULL, null, null);
        verify(zooKeeperHandler, times(1))
                .createChannelInZooKeeper(channel, EncryptionType.FULL, null);
        handler.createChannel(channel, EncryptionType.GRANULAR,
                StructuredDataAccessorStub.class.getCanonicalName(), null);
        verify(zooKeeperHandler, times(1))
                .createChannelInZooKeeper(
                        channel, EncryptionType.GRANULAR, StructuredDataAccessorStub.class);
        handler.createChannel(channel, EncryptionType.GRANULAR, null, DataFormat.JSON);
        verify(zooKeeperHandler, times(1))
                .createChannelInZooKeeper(
                        channel, EncryptionType.GRANULAR, JsonDataAccessor.class);
    }

    /**
     * Test creating channel with nonexistent accessor class
     */
    @Test
    public void testCreateChannelWithException() {
        expectedException.expect(CommonException.class);
        expectedException.expectCause(
                IsInstanceOf.instanceOf(ClassNotFoundException.class));
        handler.createChannel(channel, EncryptionType.GRANULAR, "test", null);
    }

    /**
     * Test adding re-encryption key for consumer with non-default client key type
     */
    @Test
    public void testAddReKeyWithKeyTypes() throws Exception {
        testAddReKeyWithKeyTypes(KeyType.DEFAULT);
        testAddReKeyWithKeyTypes(KeyType.PRIVATE);
        testAddReKeyWithKeyTypes(KeyType.PRIVATE_AND_PUBLIC);
        testAddReKeyWithKeyTypes(KeyType.PUBLIC);
    }

    private void testAddReKeyWithKeyTypes(KeyType keyType) throws Exception {
        handler.generateAndSaveReEncryptionKey(
                ALGORITHM,
                masterKey,
                clientKey,
                null,
                ClientType.CONSUMER,
                keyType,
                clientName,
                channel,
                null,
                null,
                null,
                null,
                null);

        verifyStatic(atLeastOnce());
        KeyUtils.generateReEncryptionKey(any(), anyString(), anyString(), eq(keyType), anyString());
    }

    /**
     * Test adding re-encryption key for consumer with non-default client key type and fields
     */
    @Test
    public void testAddReKeyWithKeyTypesAndFields() throws Exception {
        testAddReKeyWithKeyTypesAndFields(KeyType.DEFAULT);
        testAddReKeyWithKeyTypesAndFields(KeyType.PRIVATE);
        testAddReKeyWithKeyTypesAndFields(KeyType.PRIVATE_AND_PUBLIC);
        testAddReKeyWithKeyTypesAndFields(KeyType.PUBLIC);
    }

    private void testAddReKeyWithKeyTypesAndFields(KeyType keyType) throws Exception {
        handler.generateAndSaveReEncryptionKey(
                ALGORITHM,
                masterKey,
                clientKey,
                null,
                ClientType.CONSUMER,
                keyType,
                clientName,
                channel,
                null,
                null,
                fields,
                null,
                null);

        verifyStatic(atLeastOnce());
        KeyUtils.generateReEncryptionKey(any(), (KeyPair) any(),
                any(), eq(keyType), anyString());
    }

    /**
     * Test adding re-encryption key for producer with non-default key type
     */
    @Test
    public void testAddProducerReKeyWithKeyType() throws Exception {
        expectedException.expect(CommonException.class);
        expectedException.expectMessage(
                StringContains.containsString("Only available"));
        handler.generateAndSaveReEncryptionKey(
                ALGORITHM,
                masterKey,
                clientKey,
                null,
                ClientType.PRODUCER,
                KeyType.PUBLIC,
                clientName,
                channel,
                null,
                null,
                null,
                null,
                null);
    }

    /**
     * Test adding re-encryption key for producer with non-default key type and fields
     */
    @Test
    public void testAddProducerReKeyWithKeyTypeAndFields() throws Exception {
        expectedException.expect(CommonException.class);
        expectedException.expectMessage(
                StringContains.containsString("Only available"));
        handler.generateAndSaveReEncryptionKey(
                ALGORITHM,
                masterKey,
                clientKey,
                null,
                ClientType.PRODUCER,
                KeyType.PUBLIC,
                clientName,
                channel,
                null,
                null,
                fields,
                null,
                null);
    }

    /**
     * Test adding empty re-encryption key
     */
    @Test
    public void testAddEmptyReKey() throws Exception {
        WrapperReEncryptionKey emptyKey = new WrapperReEncryptionKey();

        handler.generateAndSaveReEncryptionKey(
                ALGORITHM,
                null,
                null,
                null,
                ClientType.CONSUMER,
                null,
                clientName,
                channel,
                null,
                null,
                null,
                null,
                null);
        verify(zooKeeperHandler, times(1)).saveKeyToZooKeeper(
                new KeyHolder(channel, clientName, ClientType.CONSUMER, emptyKey));

        handler.generateAndSaveReEncryptionKey(
                ALGORITHM,
                null,
                null,
                null,
                ClientType.PRODUCER,
                null,
                clientName,
                channel,
                null,
                null,
                null,
                null,
                null);
        verify(zooKeeperHandler, times(1)).saveKeyToZooKeeper(
                new KeyHolder(channel, clientName, ClientType.PRODUCER, emptyKey));

        handler.generateAndSaveReEncryptionKey(
                ALGORITHM,
                null,
                null,
                null,
                ClientType.CONSUMER,
                null,
                clientName,
                channel,
                null,
                null,
                fields,
                null,
                null);
        verify(zooKeeperHandler, times(1)).saveKeyToZooKeeper(
                KeyHolder.builder().setChannel(channel).setName(clientName)
                        .setType(ClientType.CONSUMER).setKey(emptyKey).setField("b").build());
        verify(zooKeeperHandler, times(1)).saveKeyToZooKeeper(
                KeyHolder.builder().setChannel(channel).setName(clientName)
                        .setType(ClientType.CONSUMER).setKey(emptyKey).setField("a.c").build());
    }

    /**
     * Test generating private and public EC keys
     */
    @Test
    public void testGenerateKeys() throws Exception {
        KeyPair keyPair = new KeyPair(publicKey, privateKey);
        when(KeyUtils.generateECKeyPair(any(), anyString()))
                .thenReturn(new KeyUtils.KeyPairHolder(keyPair, null));

        String privateKeyPath = "privateKey";
        String publicKeyPath = "publicKey";

        handler.generateKeyPair(ALGORITHM, curve, privateKeyPath, null);
        verifyStatic(times(1));
        KeyUtils.generateECKeyPair(eq(ALGORITHM), eq(curve));
        verifyStatic(times(1));
        KeyUtils.writeKeyPairToPEM(eq(privateKeyPath), eq(keyPair), eq(KeyType.PRIVATE));

        handler.generateKeyPair(ALGORITHM, curve, privateKeyPath, publicKeyPath);
        verifyStatic(times(2));
        KeyUtils.generateECKeyPair(eq(ALGORITHM), eq(curve));
        verifyStatic(times(2));
        KeyUtils.writeKeyPairToPEM(eq(privateKeyPath), eq(keyPair), eq(KeyType.PRIVATE));
        verifyStatic(times(1));
        KeyUtils.writeKeyPairToPEM(eq(publicKeyPath), eq(keyPair), eq(KeyType.PUBLIC));
    }
}
