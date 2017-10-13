package com.nucypher.kafka;

import org.bouncycastle.jce.provider.BouncyCastleProvider;

import java.io.Serializable;

/**
 * Commons constants
 */
public class Constants {

	public static final String BOUNCY_CASTLE_PROVIDER_NAME =
			new BouncyCastleProvider().getName();
	public static final String AES_ALGORITHM_NAME = "AES";
	public static final String KEY_FACTORY_ALGORITHM = "ECDSA";

}
