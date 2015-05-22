package de.probst.chunkedswarm.net.netty.util;

import io.netty.channel.ChannelFutureListener;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 22.05.15
 */
public final class ChannelFutureListeners {

    private ChannelFutureListeners() {
    }

    public static final ChannelFutureListener REPORT_IF_FAILED = future -> {
        if (!future.isSuccess()) {
            future.channel().pipeline().fireExceptionCaught(future.cause());
        }
    };
}
