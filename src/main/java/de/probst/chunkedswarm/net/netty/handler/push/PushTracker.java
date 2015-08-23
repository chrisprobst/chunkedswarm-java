package de.probst.chunkedswarm.net.netty.handler.push;

import de.probst.chunkedswarm.net.netty.handler.push.message.ChunkPushMessage;
import de.probst.chunkedswarm.util.BlockHeader;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 22.08.15
 */
public final class PushTracker {

    private final Consumer<PushTracker> callback;
    private final BlockHeader blockHeader;
    private final ByteBuffer payload;
    private final Map<Channel, Integer> chunkMap;

    private final AtomicInteger counter = new AtomicInteger();
    private final ConcurrentMap<Channel, ChannelFuture> successfulChannels = new ConcurrentHashMap<>();
    private final ConcurrentMap<Channel, ChannelFuture> failedChannels = new ConcurrentHashMap<>();

    static PushTracker createFrom(Consumer<PushTracker> callback,
                                  BlockHeader blockHeader,
                                  ByteBuffer payload,
                                  Map<Channel, Integer> chunkMap) {
        PushTracker pushTracker = new PushTracker(callback, blockHeader, payload, chunkMap);
        pushTracker.writeAndFlushAll();
        return pushTracker;
    }

    private void writeAndFlushAll() {
        // Start push operation for each channel
        chunkMap.forEach((c, i) -> {

            // Determine chunk index
            int chunkIndex = blockHeader.getChunkCount() == chunkMap.size() ? i : 0;

            // Create new chunk push message and write
            c.writeAndFlush(ChunkPushMessage.createFrom(blockHeader, chunkIndex, payload)).addListener(fut -> {

                // Add channel future to desired map
                ChannelFuture channelFuture = (ChannelFuture) fut;
                if (channelFuture.isSuccess()) {
                    successfulChannels.put(channelFuture.channel(), channelFuture);
                } else {
                    failedChannels.put(channelFuture.channel(), channelFuture);
                }

                // Check atomically, if all pending write requests are finished
                // If true, all results are definitely added to the maps
                if (counter.incrementAndGet() == chunkMap.size()) {
                    callback.accept(this);
                }
            });
        });
    }

    private PushTracker(Consumer<PushTracker> callback,
                        BlockHeader blockHeader,
                        ByteBuffer payload,
                        Map<Channel, Integer> chunkMap) {
        Objects.requireNonNull(callback);
        Objects.requireNonNull(blockHeader);
        Objects.requireNonNull(payload);
        Objects.requireNonNull(chunkMap);
        this.callback = callback;
        this.blockHeader = blockHeader;
        this.payload = payload;
        this.chunkMap = Collections.unmodifiableMap(chunkMap);
    }

    public boolean isCompleted() {
        return counter.get() == chunkMap.size();
    }

    public BlockHeader getBlockHeader() {
        return blockHeader;
    }

    public ByteBuffer getPayload() {
        return payload;
    }

    public Map<Channel, Integer> getChunkMap() {
        return chunkMap;
    }

    public Map<Channel, ChannelFuture> getSuccessfulChannels() {
        return Collections.unmodifiableMap(successfulChannels);
    }

    public Map<Channel, ChannelFuture> getFailedChannels() {
        return Collections.unmodifiableMap(failedChannels);
    }

    @Override
    public String toString() {
        return "PushTracker{" +
               "callback=" + callback +
               ", blockHeader=" + blockHeader +
               ", payload=" + payload +
               ", chunkMap=" + chunkMap +
               ", counter=" + counter +
               ", successfulChannels=" + successfulChannels +
               ", failedChannels=" + failedChannels +
               '}';
    }
}
