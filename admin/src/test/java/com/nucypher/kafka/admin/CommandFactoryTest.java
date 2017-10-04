package com.nucypher.kafka.admin;

import com.nucypher.crypto.EncryptionAlgorithm;
import com.nucypher.kafka.TestUtils;
import com.nucypher.kafka.admin.databind.Command;
import com.nucypher.kafka.admin.databind.CommandFactory;
import com.nucypher.kafka.admin.databind.CommandType;
import com.nucypher.kafka.clients.granular.DataFormat;
import com.nucypher.kafka.clients.granular.StructuredDataAccessorStub;
import com.nucypher.kafka.errors.CommonException;
import com.nucypher.kafka.utils.KeyType;
import com.nucypher.kafka.zk.ClientType;
import com.nucypher.kafka.zk.EncryptionType;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.verifyStatic;
import static org.powermock.api.mockito.PowerMockito.whenNew;

/**
 * Test for {@link CommandFactory}
 *
 * @author szotov
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({CommandFactory.class, AdminHandler.class})
@PowerMockIgnore("javax.management.*")
public class CommandFactoryTest {

    private static final EncryptionAlgorithm ALGORITHM = TestUtils.ENCRYPTION_ALGORITHM;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private String masterKey = "masterKey";
    private String clientKey = "clientKey";
    private String clientName = "clientName";
    private String channel = "channelName";
    private String curve = "curveName";
    private Set<String> fields = new HashSet<>();
    {
        fields.add("a.c");
        fields.add("b");
    }


    /**
     * Test one command
     */
    @Test
    public void testOneCommand() throws Exception {
        String filename = getClass().getResource("/command.json").getPath();
        List<Command> commands = CommandFactory.getCommands(filename);
        assertEquals(1, commands.size());
        Command command = commands.get(0);
        assertEquals(CommandType.ADD_KEY, command.getCommandType());
        assertEquals(ALGORITHM.getClass(), command.getEncryptionAlgorithm().getClass());
        assertEquals(masterKey, command.getMasterKey());
        assertEquals(clientKey, command.getClientKey());
        assertEquals(curve, command.getCurveName());
        assertEquals(ClientType.CONSUMER, command.getClientType());
        assertNull(command.getKeyType());
        assertEquals(clientName, command.getClientName());
        assertEquals(channel, command.getChannelName());
        assertNull(command.getExpiredDays());
        assertNull(command.getExpiredDate());
        assertNull(command.getFields());
        assertNull(command.getChannelDataAccessor());
        assertNull(command.getChannelDataFormat());
    }

    /**
     * Test multiple commands
     */
    @Test
    public void testMultipleCommands() throws Exception {
        String filename = getClass().getResource("/commands.json").getPath();
        List<Command> commands = CommandFactory.getCommands(filename);
        assertEquals(13, commands.size());
        int i = 0;
        Command command = commands.get(i);
        assertEquals(CommandType.ADD_KEY, command.getCommandType());
        assertEquals(ALGORITHM.getClass(), command.getEncryptionAlgorithm().getClass());
        assertEquals(masterKey, command.getMasterKey());
        assertEquals(clientKey, command.getClientKey());
        assertEquals(curve, command.getCurveName());
        assertEquals(ClientType.CONSUMER, command.getClientType());
        assertEquals(KeyType.PRIVATE, command.getKeyType());
        assertEquals(clientName, command.getClientName());
        assertEquals(channel, command.getChannelName());
        assertNull(command.getExpiredDays());
        assertNull(command.getExpiredDate());
        assertEquals(fields, command.getFields());
        assertNull(command.getChannelDataAccessor());
        assertNull(command.getChannelDataFormat());

        ZonedDateTime dateTime = ZonedDateTime.of(
                2017, 1, 1, 0, 0, 0, 0, ZoneId.of("Z"));
        command = commands.get(++i);
        assertEquals(CommandType.ADD_KEY, command.getCommandType());
        assertEquals(ALGORITHM.getClass(), command.getEncryptionAlgorithm().getClass());
        assertEquals(masterKey, command.getMasterKey());
        assertEquals(clientKey, command.getClientKey());
        assertNull(command.getCurveName());
        assertEquals(ClientType.PRODUCER, command.getClientType());
        assertEquals(KeyType.PUBLIC, command.getKeyType());
        assertEquals(clientName, command.getClientName());
        assertEquals(channel, command.getChannelName());
        assertNull(command.getExpiredDays());
        assertEquals(dateTime, command.getExpiredDate());
        assertNull(command.getFields());
        assertNull(command.getChannelDataAccessor());
        assertNull(command.getChannelDataFormat());

        command = commands.get(++i);
        assertEquals(CommandType.ADD_KEY, command.getCommandType());
        assertEquals(ALGORITHM.getClass(), command.getEncryptionAlgorithm().getClass());
        assertEquals(masterKey, command.getMasterKey());
        assertEquals(clientKey, command.getClientKey());
        assertNull(command.getCurveName());
        assertEquals(ClientType.PRODUCER, command.getClientType());
        assertNull(command.getKeyType());
        assertEquals(clientName, command.getClientName());
        assertEquals(channel, command.getChannelName());
        assertNull(command.getExpiredDays());
        assertEquals(dateTime, command.getExpiredDate());
        assertEquals(fields, command.getFields());
        assertEquals(StructuredDataAccessorStub.class.getCanonicalName(),
                command.getChannelDataAccessor());
        assertNull(command.getChannelDataFormat());

        int days = 10;
        command = commands.get(++i);
        assertEquals(CommandType.ADD_KEY, command.getCommandType());
        assertEquals(ALGORITHM.getClass(), command.getEncryptionAlgorithm().getClass());
        assertEquals(masterKey, command.getMasterKey());
        assertEquals(clientKey, command.getClientKey());
        assertNull(command.getCurveName());
        assertEquals(ClientType.PRODUCER, command.getClientType());
        assertNull(command.getKeyType());
        assertEquals(clientName, command.getClientName());
        assertEquals(channel, command.getChannelName());
        assertEquals(Integer.valueOf(days), command.getExpiredDays());
        assertNull(command.getExpiredDate());
        assertEquals(fields, command.getFields());
        assertNull(command.getChannelDataAccessor());
        assertEquals(DataFormat.JSON, command.getChannelDataFormat());

        command = commands.get(++i);
        assertEquals(CommandType.DELETE_KEY, command.getCommandType());
        assertEquals(ClientType.PRODUCER, command.getClientType());
        assertEquals(clientName, command.getClientName());
        assertEquals(channel, command.getChannelName());
        assertNull(command.getFields());

        command = commands.get(++i);
        assertEquals(CommandType.DELETE_KEY, command.getCommandType());
        assertEquals(ClientType.CONSUMER, command.getClientType());
        assertEquals(clientName, command.getClientName());
        assertEquals(channel, command.getChannelName());
        assertEquals(new HashSet<>(Collections.singletonList("a.c")),
                command.getFields());

        command = commands.get(++i);
        assertEquals(CommandType.DELETE_CHANNEL, command.getCommandType());
        assertEquals(channel, command.getChannelName());

        command = commands.get(++i);
        assertEquals(CommandType.ADD_CHANNEL, command.getCommandType());
        assertEquals(channel, command.getChannelName());
        assertEquals(EncryptionType.FULL, command.getChannelType());
        assertNull(command.getFields());
        assertNull(command.getChannelDataAccessor());
        assertNull(command.getChannelDataFormat());

        command = commands.get(++i);
        assertEquals(CommandType.ADD_CHANNEL, command.getCommandType());
        assertEquals(channel, command.getChannelName());
        assertEquals(EncryptionType.GRANULAR, command.getChannelType());
        assertNull(command.getFields());
        assertEquals(StructuredDataAccessorStub.class.getCanonicalName(),
                command.getChannelDataAccessor());
        assertNull(command.getChannelDataFormat());

        command = commands.get(++i);
        assertEquals(CommandType.ADD_CHANNEL, command.getCommandType());
        assertEquals(channel, command.getChannelName());
        assertEquals(EncryptionType.GRANULAR, command.getChannelType());
        assertNull(command.getFields());
        assertNull(command.getChannelDataAccessor());
        assertEquals(DataFormat.JSON, command.getChannelDataFormat());

        command = commands.get(++i);
        assertEquals(CommandType.ADD_KEY, command.getCommandType());
        assertEquals(ALGORITHM.getClass(), command.getEncryptionAlgorithm().getClass());
        assertNull(command.getMasterKey());
        assertNull(command.getClientKey());
        assertNull(command.getCurveName());
        assertEquals(ClientType.CONSUMER, command.getClientType());
        assertNull(command.getKeyType());
        assertEquals(clientName, command.getClientName());
        assertEquals(channel, command.getChannelName());
        assertNull(command.getExpiredDays());
        assertNull(command.getExpiredDate());
        assertNull(command.getFields());
        assertNull(command.getChannelDataAccessor());
        assertNull(command.getChannelDataFormat());

        String privateKeyPath = "privateKey";
        String publicKeyPath = "publicKey";
        command = commands.get(++i);
        assertEquals(CommandType.GENERATE, command.getCommandType());
        assertEquals(ALGORITHM.getClass(), command.getEncryptionAlgorithm().getClass());
        assertEquals(curve, command.getCurveName());
        assertEquals(privateKeyPath, command.getPrivateKeyPath());
        assertNull(command.getPublicKeyPath());

        command = commands.get(++i);
        assertEquals(CommandType.GENERATE, command.getCommandType());
        assertEquals(ALGORITHM.getClass(), command.getEncryptionAlgorithm().getClass());
        assertEquals(curve, command.getCurveName());
        assertEquals(privateKeyPath, command.getPrivateKeyPath());
        assertEquals(publicKeyPath, command.getPublicKeyPath());
    }

    /**
     * Test wrong commands
     */
    @Test
    public void testWrongCommands() throws Exception {
        expectedException.expect(CommonException.class);
        expectedException.expectMessage("Errors in commands parameters:\n" +
                "error in command 1: client type must not be null, master and client keys must be set together or must be null, client name must not be null, should be set only expiry date or days\n" +
                "error in command 2: should be set only data accessor class or data format\n" +
                "error in command 3: client name must not be null, client type must not be null, should be only one field for key deletion or nothing\n" +
                "error in command 4: channel must not be null\n" +
                "error in command 5: channel must not be null, should be set only data accessor class or data format\n" +
                "error in command 6: curve must not be null, private key path must not be null");
        String filename = getClass().getResource("/wrong_commands.json").getPath();
        CommandFactory.getCommands(filename);
    }

}
