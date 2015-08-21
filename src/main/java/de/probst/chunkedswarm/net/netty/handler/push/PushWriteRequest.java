package de.probst.chunkedswarm.net.netty.handler.push;

import de.probst.chunkedswarm.net.netty.handler.push.message.ChunkPushMessage;
import de.probst.chunkedswarm.util.BlockHeader;
import io.netty.channel.Channel;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 20.08.15
 */
public final class PushWriteRequest {

    private final BlockHeader block;
    private final ByteBuffer payload;
    private final Map<Channel, Integer> chunkMap;

    private ByteBuffer getChunkPayload(int chunkIndex) {
        ByteBuffer dup = payload.duplicate();
        dup.position(block.getDefaultChunkSize() * chunkIndex);
        dup.limit(block.getDefaultChunkSize() * chunkIndex + block.getChunkSize(chunkIndex));
        return dup;
    }

    public PushWriteRequest(BlockHeader block, ByteBuffer payload, Map<Channel, Integer> chunkMap) {
        Objects.requireNonNull(block);
        Objects.requireNonNull(payload);
        Objects.requireNonNull(chunkMap);
        this.block = block;
        this.payload = payload;
        this.chunkMap = Collections.unmodifiableMap(chunkMap);
    }

    public ChunkPushMessage createChunkPushMessage(Channel channel) {
        int chunkIndex = chunkMap.get(channel);
        return new ChunkPushMessage(block, block.getChunk(chunkIndex), getChunkPayload(chunkIndex));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PushWriteRequest that = (PushWriteRequest) o;

        if (!block.equals(that.block)) return false;
        if (!payload.equals(that.payload)) return false;
        return chunkMap.equals(that.chunkMap);

    }

    @Override
    public int hashCode() {
        int result = block.hashCode();
        result = 31 * result + payload.hashCode();
        result = 31 * result + chunkMap.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "PushMessage{" +
               "block=" + block +
               ", payload=" + payload +
               ", chunkMap=" + chunkMap +
               '}';
    }
}
