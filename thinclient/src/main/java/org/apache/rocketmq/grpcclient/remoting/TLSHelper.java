package org.apache.rocketmq.grpcclient.remoting;

import org.apache.rocketmq.grpcclient.utility.UtilAll;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

public class TLSHelper {
    private static final String HMAC_SHA1_ALGORITHM = "HmacSHA1";

    private TLSHelper() {
    }

    public static String sign(String accessSecret, String dateTime) throws UnsupportedEncodingException,
            NoSuchAlgorithmException,
            InvalidKeyException {
        SecretKeySpec signingKey = new SecretKeySpec(accessSecret.getBytes(StandardCharsets.UTF_8),
                HMAC_SHA1_ALGORITHM);
        Mac mac;
        mac = Mac.getInstance(HMAC_SHA1_ALGORITHM);
        mac.init(signingKey);

        return UtilAll.encodeHexString(mac.doFinal(dateTime.getBytes(StandardCharsets.UTF_8)), false);
    }
}
