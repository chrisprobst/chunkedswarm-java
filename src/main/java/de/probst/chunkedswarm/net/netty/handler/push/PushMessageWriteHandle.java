package de.probst.chunkedswarm.net.netty.handler.push;

import de.probst.chunkedswarm.net.netty.handler.push.message.PushMessage;
import de.probst.chunkedswarm.net.netty.handler.push.message.SinglePushMessage;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 20.08.15
 */
public final class PushMessageWriteHandle extends ChannelHandlerAdapter {

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof PushMessage) {
            PushMessage pushMessage = (PushMessage) msg;
            Integer chunkIndex = pushMessage.getChunkMap().get(ctx.channel());
            if (chunkIndex == null) {
                // Not meant for us
                promise.setFailure(new IllegalStateException("Chunk not for this channel"));
                return;
            }

            // Create new single push message
            SinglePushMessage singlePushMessage = new SinglePushMessage(pushMessage.getBlock(),
                                                                        pushMessage.getBlock().getChunk(chunkIndex));

            // Transfer only the single push message
            super.write(ctx, singlePushMessage, promise);
        } else {
            super.write(ctx, msg, promise);
        }
    }
}
