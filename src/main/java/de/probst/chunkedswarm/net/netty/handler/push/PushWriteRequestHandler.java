package de.probst.chunkedswarm.net.netty.handler.push;

import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 20.08.15
 */
public final class PushWriteRequestHandler extends ChannelHandlerAdapter {

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof PushWriteRequest) {
            PushWriteRequest pushMessage = (PushWriteRequest) msg;

            // Check for chunk index
            Integer chunkIndex = pushMessage.getChunkMap().get(ctx.channel());
            if (chunkIndex == null) {
                // Not meant for us
                promise.setFailure(new IllegalStateException("Chunk not for this channel"));
                return;
            }

            // Transfer only the single chunk
            super.write(ctx, pushMessage.createChunkPushMessage(chunkIndex), promise);
        } else {
            super.write(ctx, msg, promise);
        }
    }
}
