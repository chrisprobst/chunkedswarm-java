package de.probst.chunkedswarm.net.netty.util;

import io.netty.channel.Channel;
import io.netty.channel.group.ChannelGroupFuture;
import io.netty.channel.group.ChannelMatcher;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.util.concurrent.EventExecutor;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 19.08.15
 */
public final class CloseableChannelGroup extends DefaultChannelGroup {

    private volatile boolean closed = false;

    public CloseableChannelGroup(EventExecutor executor) {
        super(executor);
    }

    public CloseableChannelGroup(String name, EventExecutor executor) {
        super(name, executor);
    }

    @Override
    public boolean add(Channel channel) {
        boolean addResult = super.add(channel);

        // First add channel, than check if closed
        // Seems inefficient at first, but this way a volatile
        // gives us enough synchronization to be thread-safe
        // If true: Close right away...
        // If false: Channel will definitely be closed by the group (closed=true always happens-before close();)
        if (closed) {
            channel.close();
        }
        return addResult;
    }

    @Override
    public ChannelGroupFuture close(ChannelMatcher matcher) {
        // It is important to set the flag to true, before closing channels
        // Our invariants are: closed=true -> closed() -> add() -> closed==true?
        closed = true;
        return super.close(matcher);
    }

    @Override
    public ChannelGroupFuture close() {
        // It is important to set the flag to true, before closing channels
        // Our invariants are: closed=true -> closed() -> add() -> closed==true?
        closed = true;
        return super.close();
    }
}
