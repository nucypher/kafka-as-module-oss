package com.nucypher.kafka.zk;

import com.nucypher.kafka.clients.granular.DataFormat;
import com.nucypher.kafka.clients.granular.StructuredDataAccessor;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Class for holding information about channel
 *
 * @author szotov
 */
public class Channel {

    private String name;
    private EncryptionType type;
    private Set<String> fields;
    private Class<? extends StructuredDataAccessor> accessorClass;

    /**
     * Create simple channel with full encryption
     *
     * @param name channel name
     */
    public Channel(String name) {
        this.name = name;
        this.type = EncryptionType.FULL;
        this.fields = new HashSet<>();
    }

    /**
     * Create channel with partial encryption
     *
     * @param name          channel name
     * @param fields        list of fields
     * @param accessorClass structured data accessor class
     */
    public Channel(String name,
                   Set<String> fields,
                   Class<? extends StructuredDataAccessor> accessorClass) {
        this.name = name;
        this.type = EncryptionType.GRANULAR;
        this.fields = fields;
        this.accessorClass = accessorClass;
    }

    /**
     * @param name          channel name
     * @param type          encryption type
     * @param fields        list of fields
     * @param accessorClass structured data accessor class
     */
    public Channel(String name,
                   EncryptionType type,
                   Set<String> fields,
                   Class<? extends StructuredDataAccessor> accessorClass) {
        this.name = name;
        this.type = type;
        this.fields = fields;
        this.accessorClass = accessorClass;
    }

    /**
     * @return channel name
     */
    public String getName() {
        return name;
    }

    /**
     * @return collection of fields
     */
    public Set<String> getFields() {
        return fields;
    }

    /**
     * @return encryption type
     */
    public EncryptionType getType() {
        return type;
    }

    /**
     * @return structured data accessor class
     */
    public Class<? extends StructuredDataAccessor> getAccessorClass() {
        return accessorClass;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Channel channel = (Channel) o;
        return Objects.equals(name, channel.name) &&
                type == channel.type &&
                Objects.equals(fields, channel.fields) &&
                Objects.equals(accessorClass, channel.accessorClass);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, fields, accessorClass);
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder("Channel [");
        result.append("name=").append(name).append(", ");
        result.append("type=").append(type);
        if (type == EncryptionType.GRANULAR) {
            result.append(", ").append("fields=").append(fields);
            if (DataFormat.contains(accessorClass)) {
                DataFormat format = DataFormat.valueOf(accessorClass);
                result.append(", ").append("format=").append(format);
            }
            result.append(", ").append("accessor=").append(accessorClass.getCanonicalName());
        }
        result.append("]");
        return result.toString();
    }
}
