package com.nucypher.kafka.utils;

import com.nucypher.kafka.clients.ExtraParameters;
import com.nucypher.kafka.clients.Header;

import static com.nucypher.kafka.Constants.KEY_ALGORITHM;
import static com.nucypher.kafka.Constants.KEY_EDEK;
import static com.nucypher.kafka.Constants.KEY_IV;

/**
 */
public class MessageUtils {

    /**
     * Create {@link Header} with EDEK and IV
     *
     * @param topic           topic
     * @param extraParameters extra parameters with EDEK and IV
     * @return {@link Header}
     */
    public static Header getHeader(String topic, ExtraParameters extraParameters) {
        return Header
                .builder()
                .topic(topic)
                .description("description for " + topic)
                .build()
                .add(KEY_EDEK, (byte[]) extraParameters.getExtraParameters().get(KEY_EDEK))
                .add(KEY_IV, (byte[]) extraParameters.getExtraParameters().get(KEY_IV))
                .add(KEY_ALGORITHM, (byte[]) extraParameters.getExtraParameters().get(KEY_ALGORITHM));
    }

}
