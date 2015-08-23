package de.probst.chunkedswarm.net.netty.handler.forwarding.message;

import de.probst.chunkedswarm.util.ChunkHeader;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 23.08.15
 */
public final class ChunkForwardingMessage implements Serializable {
    
    public static ChunkForwardingMessage createFrom(ChunkHeader chunkHeader, ByteBuffer chunkPayload) {
        return new ChunkForwardingMessage(chunkHeader, chunkPayload.duplicate());
    }

    private final ChunkHeader chunkHeader;
    private transient ByteBuffer chunkPayload;

    private void writeObject(ObjectOutputStream s)
            throws java.io.IOException {
        s.defaultWriteObject();
        byte[] copy = new byte[chunkPayload.remaining()];
        chunkPayload.duplicate().get(copy);
        s.writeObject(copy);
    }

    private void readObject(ObjectInputStream s) throws IOException, ClassNotFoundException {
        s.defaultReadObject();
        chunkPayload = ByteBuffer.wrap((byte[]) s.readObject());
    }

    private ChunkForwardingMessage(ChunkHeader chunkHeader, ByteBuffer chunkPayload) {
        Objects.requireNonNull(chunkHeader);
        Objects.requireNonNull(chunkPayload);
        this.chunkHeader = chunkHeader;
        this.chunkPayload = chunkPayload;
    }

    public ChunkHeader getChunkHeader() {
        return chunkHeader;
    }

    public ByteBuffer getChunkPayload() {
        return chunkPayload;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ChunkForwardingMessage that = (ChunkForwardingMessage) o;

        if (!chunkHeader.equals(that.chunkHeader)) return false;
        return chunkPayload.equals(that.chunkPayload);

    }

    @Override
    public int hashCode() {
        int result = chunkHeader.hashCode();
        result = 31 * result + chunkPayload.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "ChunkForwardingMessage{" +
               "chunkHeader=" + chunkHeader +
               ", chunkPayload=" + chunkPayload +
               '}';
    }
}
