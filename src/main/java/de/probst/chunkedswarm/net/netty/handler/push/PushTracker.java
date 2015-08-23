package de.probst.chunkedswarm.net.netty.handler.push;

import de.probst.chunkedswarm.net.netty.handler.push.message.ChunkPushMessage;
import de.probst.chunkedswarm.net.netty.util.ChannelFutureTracker;
import de.probst.chunkedswarm.util.BlockHeader;
import io.netty.channel.Channel;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 22.08.15
 */
public final class PushTracker {

    private final BlockHeader blockHeader;
    private final ByteBuffer payload;
    private final Map<Channel, Integer> channels;
    private final ChannelFutureTracker channelFutureTracker;

    public PushTracker(Consumer<PushTracker> callback,
                       BlockHeader blockHeader,
                       ByteBuffer payload,
                       Map<Channel, Integer> channels) {
        Objects.requireNonNull(callback);
        Objects.requireNonNull(blockHeader);
        Objects.requireNonNull(payload);
        Objects.requireNonNull(channels);
        this.blockHeader = blockHeader;
        this.payload = payload;
        this.channels = Collections.unmodifiableMap(channels);

        if (channels.isEmpty()) {
            throw new IllegalArgumentException("channels.isEmpty()");
        }

        // Write all and start channel future tracker
        channelFutureTracker = new ChannelFutureTracker(channels.entrySet().stream().map(e -> {
            int chunkIndex = blockHeader.getChunkCount() == channels.size() ? e.getValue() : 0;
            return e.getKey()
                    .writeAndFlush(new ChunkPushMessage(blockHeader,
                                                        blockHeader.getChunkHeader(chunkIndex),
                                                        blockHeader.sliceChunkPayload(chunkIndex, payload)));
        }).collect(Collectors.toList()), chf -> callback.accept(PushTracker.this));
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

    public ChannelFutureTracker getChannelFutureTracker() {
        return channelFutureTracker;
    }

    @Override
    public String toString() {
        return "PushTracker{" +
               "blockHeader=" + blockHeader +
               ", payload=" + payload +
               ", channels=" + channels +
               ", channelFutureTracker=" + channelFutureTracker +
               '}';
    }
}
