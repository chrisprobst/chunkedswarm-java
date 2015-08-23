package de.probst.chunkedswarm.net.netty.util;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 23.08.15
 */
public final class ChannelFutureTracker {

    private final Map<Channel, ChannelFuture> channels;
    private final AtomicInteger counter = new AtomicInteger();
    private final ConcurrentMap<Channel, ChannelFuture> successfulChannels = new ConcurrentHashMap<>();
    private final ConcurrentMap<Channel, ChannelFuture> failedChannels = new ConcurrentHashMap<>();

    public ChannelFutureTracker(Collection<ChannelFuture> channelFutures,
                                Consumer<ChannelFutureTracker> channelFutureTrackerConsumer) {
        Objects.requireNonNull(channelFutures);
        Objects.requireNonNull(channelFutureTrackerConsumer);

        // Create channels map
        Map<Channel, ChannelFuture> tmp = new HashMap<>();
        channelFutures.forEach(f -> tmp.put(f.channel(), f));
        channels = Collections.unmodifiableMap(tmp);

        // Register handlers
        channelFutures.forEach(f -> f.addListener(fut -> {

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
                channelFutureTrackerConsumer.accept(ChannelFutureTracker.this);
            }
        }));
    }

    public boolean isCompleted() {
        return counter.get() == channels.size();
    }

    public Map<Channel, ChannelFuture> getChannels() {
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
        return "ChannelFutureTracker{" +
               "channels=" + channels +
               ", failedChannels=" + failedChannels +
               ", successfulChannels=" + successfulChannels +
               ", counter=" + counter +
               '}';
    }
}
