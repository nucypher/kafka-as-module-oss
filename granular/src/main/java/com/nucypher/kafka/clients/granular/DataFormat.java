package com.nucypher.kafka.clients.granular;

import com.nucypher.kafka.INamed;
import com.nucypher.kafka.errors.CommonException;

/**
 * Data format enum
 *
 * @author szotov
 */
public enum DataFormat implements INamed {

    /**
     * Avro
     */
    AVRO("avro", AvroDataAccessor.class),
    /**
     * Avro Schema Less
     */
    AVRO_SCHEMA_LESS("avro_schema_less", AvroSchemaLessDataAccessor.class),
    /**
     * JSON
     */
    JSON("json", JsonDataAccessor.class);

    private String shortName;
    private Class<? extends StructuredDataAccessor> accessorClass;

    DataFormat(String shortName, Class<? extends StructuredDataAccessor> accessorClass) {
        this.shortName = shortName;
        this.accessorClass = accessorClass;
    }

    @Override
    public String getShortName() {
        return shortName;
    }

    @Override
    public String getName() {
        return name();
    }

    /**
     * @return accessor class
     */
    public Class<? extends StructuredDataAccessor> getAccessorClass() {
        return accessorClass;
    }

    /**
     * Check whether one of the formats contains the accessor class
     *
     * @param accessorClass accessor class to check
     * @return result of checking
     */
    public static boolean contains(Class<? extends StructuredDataAccessor> accessorClass) {
        for (DataFormat format : values()) {
            if (format.getAccessorClass().equals(accessorClass)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Get data format from accessor class
     *
     * @param accessorClass accessor class
     * @return data format
     */
    public static DataFormat valueOf(Class<? extends StructuredDataAccessor> accessorClass) {
        for (DataFormat format : DataFormat.values()) {
            if (format.getAccessorClass().equals(accessorClass)) {
                return format;
            }
        }
        throw new CommonException(
                "No such data format with accessor class '%s'", accessorClass.getName());
    }
}
