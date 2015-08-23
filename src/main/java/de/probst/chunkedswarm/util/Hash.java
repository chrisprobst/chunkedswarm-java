package de.probst.chunkedswarm.util;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Objects;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 20.08.15
 */
public final class Hash implements Serializable {

    public static Hash computeSHA1(ByteBuffer byteBuffer) throws NoSuchAlgorithmException {
        MessageDigest digest = MessageDigest.getInstance("sha1");
        digest.update(byteBuffer);
        return new Hash(digest.digest());
    }

    private final byte[] bytes;

    public Hash(byte[] bytes) {
        Objects.requireNonNull(bytes);
        this.bytes = bytes;
    }

    public byte[] getBytes() {
        return bytes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Hash hash = (Hash) o;

        return Arrays.equals(bytes, hash.bytes);

    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }

    @Override
    public String toString() {
        return "Hash{" +
               "bytes=" + Util.bytesToHex(bytes) +
               '}';
    }
}
