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
    private final Map<Channel, Integer> channels;

    private final AtomicInteger counter = new AtomicInteger();
    private final ConcurrentMap<Channel, ChannelFuture> successfulChannels = new ConcurrentHashMap<>();
    private final ConcurrentMap<Channel, ChannelFuture> failedChannels = new ConcurrentHashMap<>();

    static PushTracker createFrom(Consumer<PushTracker> callback,
                                  BlockHeader blockHeader,
                                  ByteBuffer payload,
                                  Map<Channel, Integer> channels) {
        PushTracker pushTracker = new PushTracker(callback, blockHeader, payload, channels);
        pushTracker.writeAndFlushAll();
        return pushTracker;
    }

    private void writeAndFlushAll() {
        // Start push operation for each channel
        channels.forEach((c, i) -> {

            // Determine chunk index
            int chunkIndex = blockHeader.getChunkCount() == channels.size() ? i : 0;

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
                if (counter.incrementAndGet() == channels.size()) {
                    callback.accept(this);
                }
            });
        });
    }

    private PushTracker(Consumer<PushTracker> callback,
                        BlockHeader blockHeader,
                        ByteBuffer payload,
                        Map<Channel, Integer> channels) {
        Objects.requireNonNull(callback);
        Objects.requireNonNull(blockHeader);
        Objects.requireNonNull(payload);
        Objects.requireNonNull(channels);
        this.callback = callback;
        this.blockHeader = blockHeader;
        this.payload = payload;
        this.channels = Collections.unmodifiableMap(channels);

        if (channels.isEmpty()) {
            throw new IllegalArgumentException("channels.isEmpty()");
        }
    }

    public boolean isCompleted() {
        return counter.get() == channels.size();
    }

    public BlockHeader getBlockHeader() {
        return blockHeader;
    }

    public ByteBuffer getPayload() {
        return payload;
    }

    public Map<Channel, Integer> getChannels() {
        return channels;
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
               ", channels=" + channels +
               ", counter=" + counter +
               ", successfulChannels=" + successfulChannels +
               ", failedChannels=" + failedChannels +
               '}';
    }
}
