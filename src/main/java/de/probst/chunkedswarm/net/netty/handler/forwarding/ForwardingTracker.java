package de.probst.chunkedswarm.net.netty.handler.forwarding;

import de.probst.chunkedswarm.net.netty.handler.forwarding.message.ChunkForwardingMessage;
import de.probst.chunkedswarm.util.BlockHeader;
import de.probst.chunkedswarm.util.ChunkHeader;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

import java.nio.ByteBuffer;
import java.util.Collection;
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
public final class ForwardingTracker {

    private final Consumer<ForwardingTracker> callback;
    private final BlockHeader blockHeader;
    private final ChunkHeader chunkHeader;
    private final ByteBuffer chunkPayload;
    private final Collection<Channel> chunkSet;

    private final AtomicInteger counter = new AtomicInteger();
    private final ConcurrentMap<Channel, ChannelFuture> successfulChannels = new ConcurrentHashMap<>();
    private final ConcurrentMap<Channel, ChannelFuture> failedChannels = new ConcurrentHashMap<>();

    static ForwardingTracker createFrom(Consumer<ForwardingTracker> callback,
                                        BlockHeader blockHeader,
                                        ChunkHeader chunkHeader,
                                        ByteBuffer chunkPayload,
                                        Collection<Channel> chunkSet) {
        ForwardingTracker forwardingTracker = new ForwardingTracker(callback,
                                                                    blockHeader,
                                                                    chunkHeader,
                                                                    chunkPayload,
                                                                    chunkSet);
        forwardingTracker.writeAndFlushAll();
        return forwardingTracker;
    }

    private void writeAndFlushAll() {
        // Start forwarding operation for each channel
        chunkSet.forEach(c -> {
            // Create new chunk forwarding message and write
            c.writeAndFlush(ChunkForwardingMessage.createFrom(chunkHeader, chunkPayload)).addListener(fut -> {

                // Add channel future to desired map
                ChannelFuture channelFuture = (ChannelFuture) fut;
                if (channelFuture.isSuccess()) {
                    successfulChannels.put(channelFuture.channel(), channelFuture);
                } else {
                    failedChannels.put(channelFuture.channel(), channelFuture);
                }

                // Check atomically, if all pending write requests are finished
                // If true, all results are definitely added to the maps
                if (counter.incrementAndGet() == chunkSet.size()) {
                    callback.accept(this);
                }
            });
        });
    }

    private ForwardingTracker(Consumer<ForwardingTracker> callback,
                              BlockHeader blockHeader,
                              ChunkHeader chunkHeader,
                              ByteBuffer chunkPayload,
                              Collection<Channel> chunkSet) {
        Objects.requireNonNull(callback);
        Objects.requireNonNull(blockHeader);
        Objects.requireNonNull(chunkHeader);
        Objects.requireNonNull(chunkPayload);
        Objects.requireNonNull(chunkSet);
        this.callback = callback;
        this.blockHeader = blockHeader;
        this.chunkHeader = chunkHeader;
        this.chunkPayload = chunkPayload;
        this.chunkSet = Collections.unmodifiableCollection(chunkSet);
    }

    public boolean isCompleted() {
        return counter.get() == chunkSet.size();
    }

    public BlockHeader getBlockHeader() {
        return blockHeader;
    }

    public ChunkHeader getChunkHeader() {
        return chunkHeader;
    }

    public ByteBuffer getChunkPayload() {
        return chunkPayload;
    }

    public Collection<Channel> getChunkSet() {
        return chunkSet;
    }

    public Map<Channel, ChannelFuture> getSuccessfulChannels() {
        return Collections.unmodifiableMap(successfulChannels);
    }

    public Map<Channel, ChannelFuture> getFailedChannels() {
        return Collections.unmodifiableMap(failedChannels);
    }

    @Override
    public String toString() {
        return "ForwardingTracker{" +
               "callback=" + callback +
               ", blockHeader=" + blockHeader +
               ", chunkHeader=" + chunkHeader +
               ", chunkPayload=" + chunkPayload +
               ", chunkSet=" + chunkSet +
               ", counter=" + counter +
               ", successfulChannels=" + successfulChannels +
               ", failedChannels=" + failedChannels +
               '}';
    }
}
