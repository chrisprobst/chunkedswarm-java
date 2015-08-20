package de.probst.chunkedswarm.util;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 20.08.15
 */
public final class Hasher {

    public static final String DEFAULT_HASH_ALGORITHM = "sha1";

    private final MessageDigest sha1Digest;

    public Hasher() throws NoSuchAlgorithmException {
        this(DEFAULT_HASH_ALGORITHM);
    }

    public Hasher(String algorithm) throws NoSuchAlgorithmException {
        sha1Digest = MessageDigest.getInstance(algorithm);
    }

    public Hash computeHash(ByteBuffer byteBuffer) {
        sha1Digest.update(byteBuffer);
        Hash hash = new Hash(sha1Digest.digest());
        sha1Digest.reset();
        return hash;
    }
}
