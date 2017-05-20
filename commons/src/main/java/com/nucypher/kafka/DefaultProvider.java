package com.nucypher.kafka;

import org.bouncycastle.jce.provider.BouncyCastleProvider;

import java.security.Security;

/**
 * Class for provider initialization
 */
public class DefaultProvider {

    private static volatile boolean isInitialized = false;

    /**
     * Initialize ones per JVM
     */
    public static void initializeProvider() {
        if (!isInitialized) {
            synchronized (DefaultProvider.class) {
                // double check it's correct!
                if (!isInitialized) {
                    Security.addProvider(new BouncyCastleProvider());
                    isInitialized = true;
                }
            }
        }
    }
}
