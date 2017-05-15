package com.nucypher.kafka;

import org.bouncycastle.jce.provider.BouncyCastleProvider;

import java.io.Serializable;

/**
 * Commons constants
 */
public class Constants implements Serializable {
	
	private static final long serialVersionUID = 2464585341142328044L;
	
	public static final String SECURITY_PROVIDER_NAME = new BouncyCastleProvider().getName();
    public static final String AES_CBC_PKCS5_PADDING = "AES/CBC/PKCS5Padding";
	public static final String AES_CBC_PKCS7_PADDING = "AES/CBC/PKCS7Padding";
	public static final String AES_GCM_NO_PADDING = "AES/GCM/NoPadding";

	public static final String SYMMETRIC_ALGORITHM = "AES";

	public static final String KEY_FACTORY_ALGORITHM = "ECDSA";

	public static final String KEY_EDEK = "K1";
	public static final String KEY_IV = "K2";
	public static final String KEY_IS_COMPLEX = "K3";
	public static final String KEY_ALGORITHM = "K4";

}
