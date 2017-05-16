package com.nucypher.kafka.zk;

import com.nucypher.kafka.utils.WrapperReEncryptionKey;

import java.util.Date;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Class for holding information about key
 *
 * @author szotov
 */
public class KeyHolder {

    private String channel;
    private String name;
    private String field;
    private ClientType type;
    private Long expiredDate;
    private WrapperReEncryptionKey key;

    private KeyHolder() {

    }

    /**
     * @param channel channel name
     * @param name    user principal name
     * @param type    client type
     * @param key     {@link WrapperReEncryptionKey}
     */
    public KeyHolder(String channel, String name, ClientType type, WrapperReEncryptionKey key) {
        this.name = name;
        this.type = type;
        this.channel = channel;
        this.key = key;
    }

    /**
     * @param channel     channel name
     * @param name        user principal name
     * @param type        client type
     * @param key         {@link WrapperReEncryptionKey}
     * @param expiredDate expired date. Unix time with milliseconds
     */
    public KeyHolder(String channel, String name, ClientType type,
                     WrapperReEncryptionKey key, Long expiredDate) {
        this(channel, name, type, key);
        this.expiredDate = expiredDate;
    }

    /**
     * @param channel channel name
     * @param name    user principal name
     * @param type    client type
     * @param key     {@link WrapperReEncryptionKey}
     * @param days    lifetime in days
     */
    public KeyHolder(String channel, String name, ClientType type,
                     WrapperReEncryptionKey key, Integer days) {
        this(channel, name, type, key);
        if (days != null) {
            this.expiredDate = System.currentTimeMillis() + TimeUnit.DAYS.toMillis(days);
        }
    }

    /**
     * @return channel name
     */
    public String getChannel() {
        return channel;
    }

    /**
     * @return user principal name
     */
    public String getName() {
        return name;
    }

    /**
     * @return client type
     */
    public ClientType getType() {
        return type;
    }

    /**
     * @return expired date. Unix time with milliseconds
     */
    public Long getExpiredDate() {
        return expiredDate;
    }

    /**
     * @return the re-encryption key
     */
    public WrapperReEncryptionKey getKey() {
        return key;
    }

    /**
     * @return the field in the data structure to which this key belongs
     */
    public String getField() {
        return field;
    }

    private void setChannel(String channel) {
        this.channel = channel;
    }

    private void setName(String name) {
        this.name = name;
    }

    private void setField(String field) {
        this.field = field;
    }

    private void setType(ClientType type) {
        this.type = type;
    }

    private void setExpiredDate(Long expiredDate) {
        this.expiredDate = expiredDate;
    }

    private void setKey(WrapperReEncryptionKey key) {
        this.key = key;
    }

    /**
     * @return result of checking expired date
     */
    public boolean isExpired() {
        return expiredDate != null && expiredDate < System.currentTimeMillis();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KeyHolder keyHolder = (KeyHolder) o;
        return Objects.equals(channel, keyHolder.channel) &&
                Objects.equals(name, keyHolder.name) &&
                Objects.equals(field, keyHolder.field) &&
                type == keyHolder.type &&
                Objects.equals(expiredDate, keyHolder.expiredDate) &&
                Objects.equals(key, keyHolder.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(channel, name, field, type, expiredDate, key);
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder("Key [");
        if (channel != null) {
            result.append("channel=").append(channel).append(", ");
        }
        result.append("user=").append(name).append(", ");
        result.append("type=").append(type.toString().toLowerCase());
        if (expiredDate != null) {
            result.append(", expired=").append(new Date(expiredDate).toString());
        }
        if (field != null) {
            result.append(", field=").append(field);
        }
        result.append("] ");
        return result.toString();
    }

    /**
     * @return builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for {@link KeyHolder}
     */
    public static class Builder {

        private KeyHolder key = new KeyHolder();

        /**
         * @param name user principal name
         * @return this builder
         */
        public Builder setName(String name) {
            key.setName(name);
            return this;
        }

        /**
         * @param channel channel name
         * @return this builder
         */
        public Builder setChannel(String channel) {
            key.setChannel(channel);
            return this;
        }

        /**
         * @param field the field in the data structure to which this key belongs
         * @return this builder
         */
        public Builder setField(String field) {
            key.setField(field);
            return this;
        }

        /**
         * @param type client type
         * @return this builder
         */
        public Builder setType(ClientType type) {
            key.setType(type);
            return this;
        }

        /**
         * @param expiredDate expired date. Unix time with milliseconds
         * @return this builder
         */
        public Builder setExpiredDate(Long expiredDate) {
            key.setExpiredDate(expiredDate);
            return this;
        }

        /**
         * @param days lifetime in days
         * @return this builder
         */
        public Builder setExpiredDays(Integer days) {
            if (days != null) {
                key.setExpiredDate(System.currentTimeMillis() + TimeUnit.DAYS.toMillis(days));
            }
            return this;
        }

        /**
         * @param key the re-encryption key
         * @return this builder
         */
        public Builder setKey(WrapperReEncryptionKey key) {
            this.key.setKey(key);
            return this;
        }

        /**
         * Build {@link KeyHolder}. After return - builder is becoming broken
         *
         * @return instance of {@link KeyHolder}
         */
        public KeyHolder build() {
            KeyHolder result = key;
            key = null;
            return result;
        }

    }
}
