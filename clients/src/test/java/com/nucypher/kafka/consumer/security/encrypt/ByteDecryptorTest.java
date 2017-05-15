package com.nucypher.kafka.consumer.security.encrypt;

import com.nucypher.kafka.clients.decrypt.ByteDecryptorDeserializer;
import com.nucypher.kafka.clients.decrypt.InverseByteDecryptor;
import com.nucypher.kafka.clients.decrypt.StringByteDecryptorDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Test;

import static com.nucypher.kafka.utils.ByteUtils.*;
import static org.junit.Assert.assertEquals;


/**
 */
public class ByteDecryptorTest {

    @Test
    public void testDecryptorDeserializer(){
        ByteDecryptorDeserializer<String> decryptorDeserializer = new ByteDecryptorDeserializer<String>(
                new StringDeserializer(),
                new InverseByteDecryptor()
        );
        byte[] encryptedBytesFromKafkaBroker = invert("123".getBytes());
        assertEquals("123", decryptorDeserializer.deserialize("", encryptedBytesFromKafkaBroker));
    }

    @Test
    public void testStringDecryptor() {

        StringByteDecryptorDeserializer stringDecryptorDeserializer = new StringByteDecryptorDeserializer(new InverseByteDecryptor());

        byte[] encryptedBytesFromKafkaBroker = invert("123".getBytes());
        assertEquals("123", stringDecryptorDeserializer.deserialize("", encryptedBytesFromKafkaBroker));
    }


    @Test
    public void testEcnrypt() {
        byte k = (byte) 5;
        System.out.println(k);

        byte k2 = (byte) ~k;
        System.out.println(invert(k));


        byte[] _b = "123".getBytes();
        System.out.println(println(_b));
        System.out.println(println(invert(_b)));

        System.out.println(new String(invert(_b)));
        System.out.println(new String(invert(invert(_b))));

    }

    public String println(byte[] _array) {
        // no any Streams cause Java 1.7
        if (_array == null || _array.length == 0) {
            return "";
        }

        StringBuffer stringBuffer = new StringBuffer("[");
        for (byte _b : _array) {
            stringBuffer.append(_b).append(",");
        }
        if (stringBuffer.length() > 1) {
            return stringBuffer.deleteCharAt(stringBuffer.length() - 1).toString() + "]";
        } else {
            return "[]";
        }
    }

}
