package de.probst.chunkedswarm.net.netty.handler.discovery;

import de.probst.chunkedswarm.net.netty.handler.discovery.event.SwarmIdAcquisitionEvent;
import de.probst.chunkedswarm.net.netty.handler.discovery.event.SwarmIdRegistrationEvent;
import de.probst.chunkedswarm.net.netty.handler.discovery.message.SetCollectorAddressMessage;
import de.probst.chunkedswarm.net.netty.handler.discovery.message.SetLocalSwarmIdMessage;
import de.probst.chunkedswarm.net.netty.handler.discovery.message.UpdateNeighboursMessage;
import de.probst.chunkedswarm.util.SwarmId;
import de.probst.chunkedswarm.util.SwarmIdManager;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.group.ChannelGroup;

import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 22.05.15
 */
public final class SwarmIdRegistrationHandler extends ChannelHandlerAdapter {

    public static final long UPDATE_INTERVAL_MS = 1000;

    // The other, which use the same swarm id manager
    private final ChannelGroup channels;

    // The swarm id manager
    private final SwarmIdManager swarmIdManager;

    // The channel handler context
    private ChannelHandlerContext ctx;

    // The current update neighbours message
    private UpdateNeighboursMessage updateNeighboursMessage = new UpdateNeighboursMessage();

    // The update future is set, when neighbours get updated
    private ChannelFuture updateChannelFuture;

    // The local swarm id
    private SwarmId localSwarmId;

    private void fireSwarmIdAcquired() {
        ctx.pipeline().fireUserEventTriggered(new SwarmIdAcquisitionEvent(localSwarmId));
    }

    private void broadcastSwarmIdRegistered() {
        SwarmIdRegistrationEvent reg = new SwarmIdRegistrationEvent(this.ctx.channel(),
                                                                    localSwarmId,
                                                                    SwarmIdRegistrationEvent.Type.Registered);
        channels.forEach(c -> c.pipeline().fireUserEventTriggered(reg));
    }

    private void broadcastSwarmIdUnregistered() {
        SwarmIdRegistrationEvent unreg = new SwarmIdRegistrationEvent(this.ctx.channel(),
                                                                      localSwarmId,
                                                                      SwarmIdRegistrationEvent.Type.Unregistered);
        channels.forEach(c -> c.pipeline().fireUserEventTriggered(unreg));
    }

    private void replySwarmIdAcknowledged(SwarmIdRegistrationEvent swarmIdRegistrationEvent) {
        SwarmIdRegistrationEvent ack = new SwarmIdRegistrationEvent(this.ctx.channel(),
                                                                    localSwarmId,
                                                                    SwarmIdRegistrationEvent.Type.Acknowledged);
        swarmIdRegistrationEvent.getChannel().pipeline().fireUserEventTriggered(ack);
    }

    private void setCollectorAddress(ChannelHandlerContext ctx, SetCollectorAddressMessage setCollectorAddressMessage) {
        // We only support tcp/udp yet
        if (!(setCollectorAddressMessage.getCollectorAddress() instanceof InetSocketAddress)) {
            throw new IllegalArgumentException("!(setCollectorAddressMessage.getCollectorAddress() " +
                                               "instanceof InetSocketAddress)");
        }

        // Extract port
        int port = ((InetSocketAddress) setCollectorAddressMessage.getCollectorAddress()).getPort();

        // TODO: Cast to inet socket address, so be careful with different socket types!
        InetSocketAddress inetSocketAddress = (InetSocketAddress) ctx.channel().remoteAddress();

        // Register the new client and store local swarm id
        SwarmId newLocalSwarmId = swarmIdManager.register(new InetSocketAddress(inetSocketAddress.getAddress(), port));

        // Set the local swarm id
        localSwarmId = newLocalSwarmId;

        // Send the new local swarm id to the remote
        ctx.writeAndFlush(new SetLocalSwarmIdMessage(newLocalSwarmId))
           .addListener(fut -> {
               if (fut.isSuccess()) {
                   // Let handler chain know, that we have acquired our swarm id
                   fireSwarmIdAcquired();
                   broadcastSwarmIdRegistered();
               }
           })
           .addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
    }

    private void prepareUpdateNeighboursMessage(SwarmIdRegistrationEvent swarmIdRegistrationEvent) {
        // We are not ready to participate yet
        if (localSwarmId == null) {
            return;
        }

        // Ignore ping-back passages
        if (localSwarmId.equals(swarmIdRegistrationEvent.getSwarmId())) {
            return;
        }

        switch (swarmIdRegistrationEvent.getType()) {
            case Registered:
                updateNeighboursMessage.getAddNeighbours().add(swarmIdRegistrationEvent.getSwarmId());
                replySwarmIdAcknowledged(swarmIdRegistrationEvent);
                break;
            case Unregistered:
                updateNeighboursMessage.getRemoveNeighbours().add(swarmIdRegistrationEvent.getSwarmId());
                break;
            case Acknowledged:
                updateNeighboursMessage.getAddNeighbours().add(swarmIdRegistrationEvent.getSwarmId());
                break;
        }
    }

    private void updateNeighbours() {
        // We are not ready to participate yet
        if (localSwarmId == null) {
            return;
        }

        // No updates, skip this update
        if (updateNeighboursMessage.isEmpty()) {
            return;
        }

        // Pending update channel future, just ignore the current update
        if (updateChannelFuture != null) {
            return;
        }

        // Write the update neighbours message
        updateChannelFuture = ctx.writeAndFlush(updateNeighboursMessage)
                                 .addListener(fut -> updateChannelFuture = null)
                                 .addListener(ChannelFutureListener.CLOSE_ON_FAILURE);

        // Create a new update message
        updateNeighboursMessage = new UpdateNeighboursMessage();
    }

    public SwarmIdRegistrationHandler(ChannelGroup channels, SwarmIdManager swarmIdManager) {
        Objects.requireNonNull(channels);
        Objects.requireNonNull(swarmIdManager);
        this.channels = channels;
        this.swarmIdManager = swarmIdManager;
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof SwarmIdRegistrationEvent) {
            prepareUpdateNeighboursMessage((SwarmIdRegistrationEvent) evt);
        }

        super.userEventTriggered(ctx, evt);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        this.ctx = ctx;
        ctx.executor()
           .scheduleAtFixedRate(this::updateNeighbours, UPDATE_INTERVAL_MS, UPDATE_INTERVAL_MS, TimeUnit.MILLISECONDS);
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if (localSwarmId != null) {
            swarmIdManager.unregister(localSwarmId);
            broadcastSwarmIdUnregistered();
        }
        super.channelInactive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (localSwarmId == null && !(msg instanceof SetCollectorAddressMessage)) {
            throw new IllegalStateException("localSwarmId == null && !(msg instanceof SetCollectorAddressMessage)");
        } else if (localSwarmId == null) {
            setCollectorAddress(ctx, (SetCollectorAddressMessage) msg);
        } else {
            super.channelRead(ctx, msg);
        }
    }
}
