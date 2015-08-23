package de.probst.chunkedswarm.net.netty.handler.forwarding;

import de.probst.chunkedswarm.net.netty.handler.forwarding.message.ChunkForwardingMessage;
import de.probst.chunkedswarm.net.netty.util.ChannelFutureTracker;
import de.probst.chunkedswarm.util.BlockHeader;
import de.probst.chunkedswarm.util.ChunkHeader;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 22.08.15
 */
public final class ForwardingTracker {

    private final BlockHeader blockHeader;
    private final ChunkHeader chunkHeader;
    private final ByteBuffer chunkPayload;
    private final Collection<Channel> channels;
    private final ChannelFutureTracker channelFutureTracker;

    public ForwardingTracker(Consumer<ForwardingTracker> callback,
                             BlockHeader blockHeader,
                             ChunkHeader chunkHeader,
                             ByteBuffer chunkPayload,
                             Collection<Channel> channels) {
        Objects.requireNonNull(callback);
        Objects.requireNonNull(blockHeader);
        Objects.requireNonNull(chunkHeader);
        Objects.requireNonNull(chunkPayload);
        Objects.requireNonNull(channels);
        this.blockHeader = blockHeader;
        this.chunkHeader = chunkHeader;
        this.chunkPayload = chunkPayload;
        this.channels = Collections.unmodifiableCollection(channels);

        if (channels.isEmpty()) {
            throw new IllegalArgumentException("channels.isEmpty()");
        }

        // Write all and start channel future tracker
        Collection<ChannelFuture> cfs = channels.stream()
                                                .map(c -> c.writeAndFlush(new ChunkForwardingMessage(chunkHeader,
                                                                                                     chunkPayload)))
                                                .collect(Collectors.toList());
        channelFutureTracker = new ChannelFutureTracker(cfs, cft -> callback.accept(ForwardingTracker.this));
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

    public Collection<Channel> getChannels() {
        return channels;
    }

    public ChannelFutureTracker getChannelFutureTracker() {
        return channelFutureTracker;
    }

    @Override
    public String toString() {
        return "ForwardingTracker{" +
               "blockHeader=" + blockHeader +
               ", chunkHeader=" + chunkHeader +
               ", chunkPayload=" + chunkPayload +
               ", channels=" + channels +
               ", channelFutureTracker=" + channelFutureTracker +
               '}';
    }
}
