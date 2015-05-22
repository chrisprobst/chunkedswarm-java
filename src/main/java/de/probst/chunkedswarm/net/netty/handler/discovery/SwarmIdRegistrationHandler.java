package de.probst.chunkedswarm.net.netty.handler.discovery;

import de.probst.chunkedswarm.net.netty.util.ChannelFutureListeners;
import de.probst.chunkedswarm.util.SwarmId;
import de.probst.chunkedswarm.util.SwarmIdManager;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;

import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.Optional;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 22.05.15
 */
public final class SwarmIdRegistrationHandler extends ChannelHandlerAdapter {

    // The swarm id manager
    private final SwarmIdManager swarmIdManager;

    // The local swarm id
    private volatile Optional<SwarmId> localSwarmId = Optional.empty();

    public SwarmIdRegistrationHandler(SwarmIdManager swarmIdManager) {
        Objects.requireNonNull(swarmIdManager);
        this.swarmIdManager = swarmIdManager;
    }

    public Optional<SwarmId> getLocalSwarmId() {
        return localSwarmId;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (!localSwarmId.isPresent()) {
            if (!(msg instanceof Integer)) {
                throw new IllegalStateException("!(msg instanceof Integer)");
            }

            // TODO: Cast to inet socket address, so be careful with differeny socket types!
            InetSocketAddress inetSocketAddress = (InetSocketAddress) ctx.channel().remoteAddress();

            // Register the new client and store local swarm id
            SwarmId newLocalSwarmId = swarmIdManager.register(new InetSocketAddress(inetSocketAddress.getAddress(),
                                                                                    (Integer) msg));

            // Set the local swarm id
            localSwarmId = Optional.of(newLocalSwarmId);

            // Send the new local swarm id to the remote
            ctx.writeAndFlush(newLocalSwarmId).addListener(ChannelFutureListeners.REPORT_IF_FAILED);
        } else {
            super.channelRead(ctx, msg);
        }
    }
}
