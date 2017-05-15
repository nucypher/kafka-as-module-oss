package com.nucypher.kafka.admin;

import com.nucypher.kafka.INamed;
import com.nucypher.kafka.clients.granular.DataFormat;
import com.nucypher.kafka.utils.EncryptionAlgorithm;
import com.nucypher.kafka.utils.KeyType;
import com.nucypher.kafka.zk.ClientType;
import com.nucypher.kafka.zk.EncryptionType;
import joptsimple.BuiltinHelpFormatter;
import joptsimple.OptionDescriptor;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpecBuilder;
import joptsimple.ValueConverter;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.InvalidAlgorithmParameterException;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Command line interface for control keys
 *
 * @author szotov
 */
public class Console {

    /**
     * CLI
     *
     * @param args run with --help option to see available options
     * @throws IOException                        if problem with serialization or parsing data
     * @throws InvalidAlgorithmParameterException if provider not found
     */
    @SuppressWarnings("unchecked")
    public static void main(String[] args)
            throws InvalidAlgorithmParameterException, IOException {
        OptionParser parser = getOptionParser();

        OptionSet options = parser.parse(args);
        if (options.has("help")) {
            parser.printHelpOn(System.out);
            return;
        }

        if (options.has("gn")) {
            AdminHandler handler = new AdminHandler(null);
            handler.generateKeyPair(
                    (EncryptionAlgorithm) options.valueOf("alg"),
                    (String) options.valueOf("cvn"),
                    (String) options.valueOf("prk"),
                    (String) options.valueOf("pbk"));
            return;
        }

        Properties properties = new Properties();
        try (InputStream stream = new FileInputStream((String) options.valueOf("cfg"))) {
            properties.load(stream);
        }
        try (AdminZooKeeperHandler zooKeeper = new AdminZooKeeperHandler(properties)) {
            AdminHandler handler = new AdminHandler(zooKeeper);
            if (options.has("ls")) {
                handler.listAll(options.has("oc"));
            } else if (options.has("adk")) {
                handler.generateAndSaveReEncryptionKey(
                        (EncryptionAlgorithm) options.valueOf("alg"),
                        (String) options.valueOf("mk"),
                        (String) options.valueOf("ck"),
                        (String) options.valueOf("cvn"),
                        (ClientType) options.valueOf("ct"),
                        (KeyType) options.valueOf("ckt"),
                        (String) options.valueOf("cn"),
                        (String) options.valueOf("ch"),
                        (Integer) options.valueOf("d"),
                        (ZonedDateTime) options.valueOf("ex"),
                        (List<String>) options.valuesOf("f"),
                        (String) options.valueOf("cac"),
                        (DataFormat) options.valueOf("cdf"));
            } else if (options.has("rmk")) {
                handler.deleteReEncryptionKey(
                        (ClientType) options.valueOf("ct"),
                        (String) options.valueOf("cn"),
                        (String) options.valueOf("ch"),
                        (String) options.valueOf("f"));
            } else if (options.has("adc")) {
                handler.createChannel(
                        (String) options.valueOf("ch"),
                        (EncryptionType) options.valueOf("cht"),
                        (String) options.valueOf("cac"),
                        (DataFormat) options.valueOf("cdf"));
            } else if (options.has("rmc")) {
                handler.deleteChannel((String) options.valueOf("ch"));
            } else {
                parser.printHelpOn(System.out);
            }
        }


    }

    private static OptionParser getOptionParser() {
        List<String> addKeySynonyms = Arrays.asList("add-key", "create-key", "adk");
        List<String> removeKeySynonyms = Arrays.asList("delete-key", "remove-key", "rmk");
        List<String> addChannelSynonyms = Arrays.asList("add-channel", "create-channel", "adc");
        List<String> removeChannelSynonyms = Arrays.asList("delete-channel", "remove-channel", "rmc");
        List<String> generateKeySynonyms = Arrays.asList("generate", "gn");
        List<String> listSynonyms = Arrays.asList("list", "ls");
        List<String> helpSynonyms = Arrays.asList("help", "h");

        String addKeyCommand = addKeySynonyms.get(0);
        String removeKeyCommand = removeKeySynonyms.get(0);
        String addChannelCommand = addChannelSynonyms.get(0);
        String removeChannelCommand = removeChannelSynonyms.get(0);
        String generateKeyCommand = generateKeySynonyms.get(0);
        String listCommand = listSynonyms.get(0);
        String helpKeyCommand = helpSynonyms.get(0);

        List<String> commands = Arrays.asList(addKeyCommand, removeKeyCommand, generateKeyCommand,
                listCommand, helpKeyCommand, addChannelCommand, removeChannelCommand);

        OptionParser parser = new OptionParser();
        parser.acceptsAll(helpSynonyms, "print this help").forHelp();

        parser.acceptsAll(Arrays.asList("config", "cfg"), "path to the configuration file")
                .withRequiredArg()
                .defaultsTo("config.properties");

        OptionSpecBuilder generateBuilder = parser.acceptsAll(generateKeySynonyms,
                "generate key pair and save it to the file or files");
        OptionSpecBuilder addKeyBuilder = parser.acceptsAll(addKeySynonyms,
                "generate and save the re-encryption key to the storage");
        OptionSpecBuilder deleteKeyBuilder = parser.acceptsAll(removeKeySynonyms,
                "delete the re-encryption key from the storage");
        OptionSpecBuilder addChannelBuilder = parser.acceptsAll(addChannelSynonyms,
                "create the channel in the storage");
        OptionSpecBuilder deleteChannelBuilder = parser.acceptsAll(removeChannelSynonyms,
                "delete the channel from the storage");
        parser.acceptsAll(listSynonyms,
                "list all re-encryption keys or channels in the storage")
                .requiredUnless("h", filterCommands(commands, listCommand))
                .availableUnless("h", filterCommands(commands, listCommand));
        addKeyBuilder.requiredUnless("h", filterCommands(commands, addKeyCommand))
                .availableUnless("h", filterCommands(commands, addKeyCommand));
        deleteKeyBuilder.requiredUnless("h", filterCommands(commands, removeKeyCommand))
                .availableUnless("h", filterCommands(commands, removeKeyCommand));
        addChannelBuilder.requiredUnless("h", filterCommands(commands, addChannelCommand))
                .availableUnless("h", filterCommands(commands, addChannelCommand));
        deleteChannelBuilder.requiredUnless("h", filterCommands(commands, removeChannelCommand))
                .availableUnless("h", filterCommands(commands, removeChannelCommand));
        generateBuilder.requiredUnless("h", filterCommands(commands, generateKeyCommand))
                .availableUnless("h", filterCommands(commands, generateKeyCommand));

        parser.acceptsAll(Arrays.asList("client-type", "ct"), "client type")
                .requiredIf(addKeyCommand, removeKeyCommand)
                .availableIf(addKeyCommand, removeKeyCommand)
                .withRequiredArg()
                .withValuesConvertedBy(new NamedConverter<>(ClientType::values));

        parser.acceptsAll(Arrays.asList("client-key", "ck"),
                "path to the client private or public (only for consumer) key file")
                .availableIf(addKeyCommand)
                .withRequiredArg();

        parser.acceptsAll(Arrays.asList("master-key", "mk"),
                "path to the master private key file")
                .requiredIf("ck")
                .availableIf("ck")
                .withRequiredArg();

        parser.acceptsAll(Arrays.asList("algorithm", "alg"), "encryption algorithm")
                .availableIf(addKeyCommand, generateKeyCommand)
                .withRequiredArg()
                .withValuesConvertedBy(new NamedConverter<>(EncryptionAlgorithm::values))
                .defaultsTo(EncryptionAlgorithm.ELGAMAL);

        parser.acceptsAll(Arrays.asList("curve-name", "cvn"), "elliptic curve name")
                .requiredIf(generateKeyCommand)
                .availableIf(addKeyCommand, generateKeyCommand)
                .withRequiredArg();

        parser.acceptsAll(Arrays.asList("client-name", "cn"), "client principal name")
                .requiredIf(addKeyCommand, removeKeyCommand)
                .availableIf(addKeyCommand, removeKeyCommand)
                .withRequiredArg();

        parser.acceptsAll(Arrays.asList("channel", "ch"),
                "channel which is used for the key. If not set then key is using for all channels")
                .requiredIf(addChannelCommand, removeChannelCommand)
                .availableIf(addKeyCommand, removeKeyCommand, addChannelCommand, removeChannelCommand)
                .withRequiredArg();

        parser.acceptsAll(Arrays.asList("channel-type", "cht"),
                "channel encryption type")
                .availableIf(addChannelCommand)
                .withRequiredArg()
                .withValuesConvertedBy(new NamedConverter<>(EncryptionType::values))
                .defaultsTo(EncryptionType.FULL);

        OptionSpecBuilder daysBuilder = parser
                .acceptsAll(Arrays.asList("days", "d"), "lifetime of the re-encryption key in days")
                .availableIf(addKeyCommand);
        parser.acceptsAll(Arrays.asList("expired", "ex"),
                "expired date of the re-encryption key in ISO-8601 format")
                .availableIf(addKeyCommand)
                .availableUnless("d")
                .withRequiredArg()
                .withValuesConvertedBy(new ZonedDateTimeConverter());
        daysBuilder.availableUnless("ex")
                .withRequiredArg()
                .ofType(Integer.class);

        parser.acceptsAll(Arrays.asList("private-key", "prk"),
                "path to the private key file")
                .requiredIf(generateKeyCommand)
                .availableIf(generateKeyCommand)
                .withRequiredArg();

        parser.acceptsAll(Arrays.asList("public-key", "pbk"),
                "path to the public key file")
                .availableIf(generateKeyCommand)
                .withRequiredArg();

        parser.acceptsAll(Arrays.asList("key-type", "kt"),
                "generated key type")
                .availableIf(generateKeyCommand)
                .withRequiredArg()
                .withValuesConvertedBy(new NamedConverter<>(KeyType::values));

        parser.acceptsAll(Arrays.asList("only-channels", "oc"), "list only channels")
                .availableIf(listCommand);

        parser.acceptsAll(Arrays.asList("client-key-type", "ckt"),
                "client's key type (only for consumer)")
                .availableIf(addKeyCommand)
                .withRequiredArg()
                .withValuesConvertedBy(new NamedConverter<>(KeyType::values));

        parser.acceptsAll(Arrays.asList("field", "f"),
                "the field in the data structure to which this key belongs. " +
                        "Adding key: may be set multiple values. " +
                        "Deleting key: if value is null and channel type is GRANULAR " +
                        "then will be deleted all subkeys")
                .availableIf(addKeyCommand, removeKeyCommand)
                .withRequiredArg();

        OptionSpecBuilder classBuilder = parser
                .acceptsAll(Arrays.asList("channel-accessor-class", "cac"),
                        "accessor class name (only for channels with granular encryption type " +
                                "or keys with fields)")
                .availableIf(addKeyCommand, addChannelCommand);
        parser.acceptsAll(Arrays.asList("channel-data-format", "cdf"),
                "data format (only for channels with granular encryption type " +
                        "or keys with fields)")
                .availableIf(addKeyCommand, addChannelCommand)
                .availableUnless("cac")
                .withRequiredArg()
                .withValuesConvertedBy(new NamedConverter<>(DataFormat::values));
        classBuilder.availableUnless("cdf").withRequiredArg();

        parser.formatHelpWith(new CustomHelpFormatter(120, 4, commands));
        return parser;
    }

    private static String[] filterCommands(List<String> commands, String exclude) {
        List<String> filtered = commands.stream()
                .filter(x -> !exclude.equals(x))
                .collect(Collectors.toList());
        return filtered.toArray(new String[filtered.size()]);
    }

    private static class NamedConverter<T extends INamed> implements ValueConverter<T> {

        private Supplier<T[]> supplier;

        /**
         * @param supplier function that returns array of available values
         */
        public NamedConverter(Supplier<T[]> supplier) {
            this.supplier = supplier;
        }

        @Override
        public T convert(String value) {
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

        @SuppressWarnings("unchecked")
        @Override
        public Class<T> valueType() {
            return (Class<T>) supplier.get()[0].getClass();
        }

        @Override
        public String valuePattern() {
            Set<String> values = new LinkedHashSet<>();
            for (T named : supplier.get()) {
//				values.add(named.toString());
                values.add(named.getName().toLowerCase());
                values.add(named.getShortName());
            }

            return "[" + values.stream().collect(Collectors.joining(", ")) + "]";
        }

    }

    private static class ZonedDateTimeConverter implements ValueConverter<ZonedDateTime> {

        @Override
        public ZonedDateTime convert(String value) {
            return ZonedDateTime.parse(value);
        }

        @Override
        public Class<? extends ZonedDateTime> valueType() {
            return ZonedDateTime.class;
        }

        @Override
        public String valuePattern() {
            return "ISO-8601";
        }

    }

    private static class CustomHelpFormatter extends BuiltinHelpFormatter {

        private final List<String> commands;

        /**
         * @param desiredOverallWidth         {@link BuiltinHelpFormatter#BuiltinHelpFormatter(int, int)}
         * @param desiredColumnSeparatorWidth {@link BuiltinHelpFormatter#BuiltinHelpFormatter(int, int)}
         * @param commands                    main commands
         */
        CustomHelpFormatter(int desiredOverallWidth, int desiredColumnSeparatorWidth, List<String> commands) {
            super(desiredOverallWidth, desiredColumnSeparatorWidth);
            this.commands = commands;
        }

        @Override
        public String format(Map<String, ? extends OptionDescriptor> options) {
            Map<String, OptionDescriptor> commandOptions = new HashMap<>();
            Map<String, OptionDescriptor> otherOptions = new HashMap<>();

            for (Entry<String, ? extends OptionDescriptor> entry : options.entrySet()) {
                if (entry.getValue().options()
                        .stream()
                        .filter(commands::contains)
                        .count() > 0) {
                    commandOptions.put(entry.getKey(), entry.getValue());
                } else {
                    otherOptions.put(entry.getKey(), entry.getValue());
                }
                if (entry.getValue().representsNonOptions()) {
                    commandOptions.put(entry.getKey(), entry.getValue());
                }
            }

            return "Commands\n" +
                    super.format(commandOptions) +
                    '\n' +
                    "Other parameters\n" +
                    super.format(otherOptions);
        }
    }
}
