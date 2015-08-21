package de.probst.chunkedswarm.net.netty.handler.push.message;

import de.probst.chunkedswarm.util.BlockHeader;
import de.probst.chunkedswarm.util.ChunkHeader;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 20.08.15
 */
public final class ChunkPushMessage implements Serializable {

    private final BlockHeader block;
    private final ChunkHeader chunk;
    private final byte[] chunkPayload;

    public ChunkPushMessage(BlockHeader block, ChunkHeader chunk, ByteBuffer chunkPayload) {
        Objects.requireNonNull(block);
        Objects.requireNonNull(chunk);
        Objects.requireNonNull(chunkPayload);
        this.block = block;
        this.chunk = chunk;
        this.chunkPayload = new byte[chunkPayload.remaining()];
        chunkPayload.get(this.chunkPayload);
    }

    public BlockHeader getBlock() {
        return block;
    }

    public ChunkHeader getChunk() {
        return chunk;
    }

    public ByteBuffer getChunkPayload() {
        return ByteBuffer.wrap(chunkPayload);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ChunkPushMessage that = (ChunkPushMessage) o;

        if (!block.equals(that.block)) {
            return false;
        }
        if (!chunk.equals(that.chunk)) {
            return false;
        }
        return Arrays.equals(chunkPayload, that.chunkPayload);

    }

    @Override
    public int hashCode() {
        int result = block.hashCode();
        result = 31 * result + chunk.hashCode();
        result = 31 * result + Arrays.hashCode(chunkPayload);
        return result;
    }

    @Override
    public String toString() {
        return "ChunkPushMessage{" +
               "block=" + block +
               ", chunk=" + chunk +
               ", chunkPayload=" + Arrays.toString(chunkPayload) +
               '}';
    }
}
