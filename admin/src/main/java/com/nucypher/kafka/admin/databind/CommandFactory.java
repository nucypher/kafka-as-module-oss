package com.nucypher.kafka.admin.databind;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.nucypher.crypto.EncryptionAlgorithm;
import com.nucypher.kafka.INamed;
import com.nucypher.kafka.clients.granular.DataFormat;
import com.nucypher.kafka.errors.CommonException;
import com.nucypher.kafka.utils.EncryptionAlgorithmUtils;
import com.nucypher.kafka.utils.KeyType;
import com.nucypher.kafka.zk.ClientType;
import com.nucypher.kafka.zk.EncryptionType;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

/**
 * Factory for parsing command and parameters from file
 */
public class CommandFactory {

    private static final ObjectMapper MAPPER;

    static {
        SimpleModule module = new SimpleModule()
                .addDeserializer(ClientType.class,
                        new NamedDeserializer<>(ClientType::values))
                .addDeserializer(EncryptionType.class,
                        new NamedDeserializer<>(EncryptionType::values))
                .addDeserializer(KeyType.class,
                        new NamedDeserializer<>(KeyType::values))
                .addDeserializer(DataFormat.class,
                        new NamedDeserializer<>(DataFormat::values))
                .addDeserializer(ZonedDateTime.class,
                        new ZonedDateTimeDeserializer())
                .addDeserializer(EncryptionAlgorithm.class,
                        new EncryptionAlgorithmDeserializer());
        MAPPER = new ObjectMapper();
        MAPPER.setPropertyNamingStrategy(PropertyNamingStrategy.KEBAB_CASE);
        MAPPER.registerModule(module);
    }

    /**
     * Parse file and return commands
     *
     * @param filePath file with commands and parameters
     * @return list of commands
     */
    public static List<Command> getCommands(String filePath) {
        List<Command> commands;
        try {
            JsonNode root = MAPPER.readTree(new FileInputStream(filePath));
            if (root.isArray()) {
                commands = MAPPER.readerFor(
                        new TypeReference<List<Command>>(){}).readValue(root);
            } else if (root.isObject()) {
                commands = Collections.singletonList(
                        MAPPER.readerFor(Command.class).readValue(root));
            } else {
                throw new CommonException("Unexpected JSON type");
            }
        } catch (IOException e) {
            throw new CommonException(e);
        }
        validateCommands(commands);
        return commands;
    }

    private static void validateCommands(List<Command> commands) {
        boolean valid = true;
        StringBuilder errors = new StringBuilder("Errors in commands parameters:\n");
        for (int i = 0; i < commands.size(); i++) {
            Command command = commands.get(i);
            if (command.getCommandType() == null) {
                valid = false;
                errors.append("error in command ")
                        .append(i + 1)
                        .append(": ")
                        .append(" command type must not be null\n");
                continue;
            }
            String error;
            switch (command.getCommandType()) {
                case ADD_KEY:
                    error = validateAddKeyCommand(command);
                    break;
                case DELETE_KEY:
                    error = validateDeleteKeyCommand(command);
                    break;
                case ADD_CHANNEL:
                    error = validateAddChannelCommand(command);
                    break;
                case DELETE_CHANNEL:
                    error = validateDeleteChannelCommand(command);
                    break;
                case GENERATE:
                    error = validateGenerateCommand(command);
                    break;
                default:
                    error = String.format("Unrecognized command '%s'",
                            command.getCommandType());
            }
            if (error != null) {
                valid = false;
                errors.append("error in command ")
                        .append(i + 1)
                        .append(": ")
                        .append(error)
                        .append('\n');
            }
        }
        if (!valid) {
            throw new CommonException(errors.toString());
        }
    }

    //TODO refactor all validation
    private static String validateAddKeyCommand(Command command) {
        boolean valid = true;
        StringBuilder errors = new StringBuilder();
        if (command.getClientType() == null) {
            errors.append("client type must not be null");
            valid = false;
        }
        if (command.getMasterKey() == null &&
                command.getClientKey() != null ||
                command.getMasterKey() != null &&
                        command.getClientKey() == null) {
            if (!valid) {
                errors.append(", ");
            }
            errors.append("master and client keys must be set together or must be null");
            valid = false;
        }
        if (command.getClientName() == null) {
            if (!valid) {
                errors.append(", ");
            }
            errors.append("client name must not be null");
            valid = false;
        }
        if (command.getExpiredDays() != null &&
                command.getExpiredDate() != null) {
            if (!valid) {
                errors.append(", ");
            }
            errors.append("should be set only expiry date or days");
            valid = false;
        }
        if (command.getChannelDataAccessor() != null &&
                command.getChannelDataFormat() != null) {
            if (!valid) {
                errors.append(", ");
            }
            errors.append("should be set only data accessor class or data format");
            valid = false;
        }
        if (!valid) {
            return errors.toString();
        }
        return null;
    }

    private static String validateDeleteKeyCommand(Command command) {
        boolean valid = true;
        StringBuilder errors = new StringBuilder();
        if (command.getClientName() == null) {
            errors.append("client name must not be null");
            valid = false;
        }
        if (command.getClientType() == null) {
            if (!valid) {
                errors.append(", ");
            }
            errors.append("client type must not be null");
            valid = false;
        }
        if (command.getFields() != null && command.getFields().size() > 1) {
            if (!valid) {
                errors.append(", ");
            }
            errors.append("should be only one field for key deletion or nothing");
            valid = false;
        }
        if (!valid) {
            return errors.toString();
        }
        return null;
    }

    private static String validateAddChannelCommand(Command command) {
        boolean valid = true;
        StringBuilder errors = new StringBuilder();
        if (command.getChannelName() == null) {
            errors.append("channel must not be null");
            valid = false;
        }
        if (command.getChannelDataAccessor() != null &&
                command.getChannelDataFormat() != null) {
            if (!valid) {
                errors.append(", ");
            }
            errors.append("should be set only data accessor class or data format");
            valid = false;
        }

        if (!valid) {
            return errors.toString();
        }
        return null;
    }

    private static String validateDeleteChannelCommand(Command command) {
        if (command.getChannelName() == null) {
            return "channel must not be null";
        }
        return null;
    }

    private static String validateGenerateCommand(Command command) {
        boolean valid = true;
        StringBuilder errors = new StringBuilder();
        if (command.getCurveName() == null) {
            errors.append("curve must not be null");
            valid = false;
        }
        if (command.getPrivateKeyPath() == null) {
            if (!valid) {
                errors.append(", ");
            }
            errors.append("private key path must not be null");
            valid = false;
        }

        if (!valid) {
            return errors.toString();
        }
        return null;
    }

    private static class NamedDeserializer<T extends INamed> extends JsonDeserializer<T> {

        private Supplier<T[]> supplier;

        /**
         * @param supplier function that returns array of available values
         */
        public NamedDeserializer(Supplier<T[]> supplier) {
            this.supplier = supplier;
        }

        @Override
        public T deserialize(JsonParser jsonParser,
                             DeserializationContext context) throws IOException {
            String value = jsonParser.getValueAsString();
            if (value == null) {
                return null;
            }
            for (T named : supplier.get()) {
                if (named.getName().toLowerCase().equals(value.toLowerCase()) ||
                        named.getShortName().toLowerCase().equals(value.toLowerCase())) {
                    return named;
                }
            }
            throw new IllegalArgumentException("No named value for " + value);
        }
    }

    private static class ZonedDateTimeDeserializer extends JsonDeserializer<ZonedDateTime> {

        @Override
        public ZonedDateTime deserialize(JsonParser jsonParser,
                                         DeserializationContext context) throws IOException {
            String value = jsonParser.getValueAsString();
            return ZonedDateTime.parse(value);
        }

    }

    private static class EncryptionAlgorithmDeserializer
            extends JsonDeserializer<EncryptionAlgorithm> {

        @Override
        public EncryptionAlgorithm deserialize(JsonParser jsonParser,
                                               DeserializationContext context) throws IOException {
            String value = jsonParser.getValueAsString();
            if (value == null) {
                return null;
            }
            return EncryptionAlgorithmUtils.getEncryptionAlgorithm(value);
        }

    }
}
