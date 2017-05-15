package com.nucypher.kafka.clients;

import lombok.*;

import java.io.Serializable;
import java.util.Map;

/**
 *
 */
@Getter
@Setter
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class Config implements Serializable {
    private Map<String, Object> configs;
    private boolean isKey;
}
