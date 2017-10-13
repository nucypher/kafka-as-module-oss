package com.nucypher.kafka.utils;

import com.nucypher.kafka.Constants;
import com.nucypher.kafka.errors.CommonException;

import org.bouncycastle.crypto.Digest;
import org.bouncycastle.crypto.digests.SHA256Digest;
import org.bouncycastle.crypto.generators.HKDFBytesGenerator;
import org.bouncycastle.crypto.params.HKDFParameters;
import org.bouncycastle.jce.interfaces.ECPrivateKey;
import org.bouncycastle.jce.spec.ECParameterSpec;
import org.bouncycastle.jce.spec.ECPrivateKeySpec;

import java.math.BigInteger;
import java.nio.charset.Charset;
import java.security.KeyFactory;
import java.security.PrivateKey;

/**
 * EC key generator using HKDF instantiated with HMAC-SHA256. It is initialized
 * by a master EC private key.
 */
public class SubkeyGenerator {

	public final static Digest DIGEST = new SHA256Digest();

	// /**
	// * Random, non-secret, re-usable salt, as described in RFC5869, Section
	// 3.1.
	// */
	// private final static byte[] SALT =
	// { (byte) 0x07, (byte) 0xfc, (byte) 0xc0, (byte) 0xf0,
	// (byte) 0xeb, (byte) 0xce, (byte) 0xd2, (byte) 0x17,
	// (byte) 0x10, (byte) 0x62, (byte) 0x6b, (byte) 0xcc,
	// (byte) 0xa2, (byte) 0x5b, (byte) 0x9c, (byte) 0x2a };


	public static PrivateKey deriveKey(PrivateKey privateKey, byte[] data, byte[] salt) {

		byte[] inputKeyMaterial = privateKey.getEncoded();

		HKDFParameters params = new HKDFParameters(inputKeyMaterial, salt, data);

		HKDFBytesGenerator hkdf = new HKDFBytesGenerator(DIGEST);

		hkdf.init(params);

		ECParameterSpec parameters = ((ECPrivateKey) privateKey).getParameters();
		BigInteger n = parameters.getN();
		int bitLength = n.bitLength();
		int byteLength = bitLength / 8 + (bitLength % 8 == 0 ? 0 : 1);

		byte[] bytes = new byte[byteLength];
		hkdf.generateBytes(bytes, 0, byteLength);
		BigInteger generatedD = new BigInteger(bytes).mod(n);

		ECPrivateKeySpec privateKeySpec = new ECPrivateKeySpec(generatedD, parameters);

		try {
			KeyFactory keyFactory = KeyFactory.getInstance(Constants.KEY_FACTORY_ALGORITHM,
					Constants.BOUNCY_CASTLE_PROVIDER_NAME);
			return keyFactory.generatePrivate(privateKeySpec);
		} catch (Exception e) {
			throw new CommonException(e);
		}

	}
	
	public static PrivateKey deriveKey(PrivateKey privateKey, String data, byte[] salt) {
		return deriveKey(privateKey, data.getBytes(Charset.forName("UTF8")), salt);
	}
	
	public static PrivateKey deriveKey(PrivateKey privateKey, String data) {
		return deriveKey(privateKey, data.getBytes(Charset.forName("UTF8")), null);
	}
	
	public static PrivateKey deriveKey(PrivateKey privateKey, byte[] data) {
		return deriveKey(privateKey, data, null);
	}

}
