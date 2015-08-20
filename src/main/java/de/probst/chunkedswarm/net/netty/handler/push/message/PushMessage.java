package de.probst.chunkedswarm.net.netty.handler.push.message;

import de.probst.chunkedswarm.util.Block;
import io.netty.channel.Channel;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 20.08.15
 */
public final class PushMessage {

    private final Block block;
    private final Map<Channel, Integer> chunkMap;

    public PushMessage(Block block, Map<Channel, Integer> chunkMap) {
        Objects.requireNonNull(block);
        Objects.requireNonNull(chunkMap);
        this.block = block;
        this.chunkMap = Collections.unmodifiableMap(chunkMap);
    }

    public Block getBlock() {
        return block;
    }

    public Map<Channel, Integer> getChunkMap() {
        return chunkMap;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PushMessage that = (PushMessage) o;

        if (!block.equals(that.block)) return false;
        return chunkMap.equals(that.chunkMap);

    }

    @Override
    public int hashCode() {
        int result = block.hashCode();
        result = 31 * result + chunkMap.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "PushMessage{" +
               "block=" + block +
               ", chunkMap=" + chunkMap +
               '}';
    }
}
