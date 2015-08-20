package de.probst.chunkedswarm.net.netty.handler.push.message;

import de.probst.chunkedswarm.util.Block;
import de.probst.chunkedswarm.util.Chunk;

import java.io.Serializable;
import java.util.Objects;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 20.08.15
 */
public final class SinglePushMessage implements Serializable {

    private final Block block;
    private final Chunk chunk;

    public SinglePushMessage(Block block, Chunk chunk) {
        Objects.requireNonNull(block);
        Objects.requireNonNull(chunk);
        this.block = block;
        this.chunk = chunk;
    }

    public Block getBlock() {
        return block;
    }

    public Chunk getChunk() {
        return chunk;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SinglePushMessage that = (SinglePushMessage) o;

        if (!block.equals(that.block)) return false;
        return chunk.equals(that.chunk);

    }

    @Override
    public int hashCode() {
        int result = block.hashCode();
        result = 31 * result + chunk.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "SinglePushMessage{" +
               "block=" + block +
               ", chunk=" + chunk +
               '}';
    }
}
