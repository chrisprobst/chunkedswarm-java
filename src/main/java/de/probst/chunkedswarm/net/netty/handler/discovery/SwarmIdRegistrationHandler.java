package de.probst.chunkedswarm.net.netty.handler.discovery;

import de.probst.chunkedswarm.net.netty.handler.discovery.event.SwarmIDAcquisitionEvent;
import de.probst.chunkedswarm.net.netty.handler.discovery.event.SwarmIDRegistrationAcknowledgementEvent;
import de.probst.chunkedswarm.net.netty.handler.discovery.event.SwarmIDRegistrationEvent;
import de.probst.chunkedswarm.net.netty.handler.discovery.event.UpdateNeighboursEvent;
import de.probst.chunkedswarm.net.netty.handler.discovery.message.SetCollectorAddressMessage;
import de.probst.chunkedswarm.net.netty.handler.discovery.message.SetLocalSwarmIDMessage;
import de.probst.chunkedswarm.net.netty.handler.discovery.message.UpdateNeighboursMessage;
import de.probst.chunkedswarm.util.SwarmID;
import de.probst.chunkedswarm.util.SwarmIDManager;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.group.ChannelGroup;

import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Handler sends to owner channel:
 * - SwarmIDAcquisitionEvent
 * - UpdateNeighboursEvent
 * <p>
 * Handler broadcasts to parent channel and all other channels:
 * - SwarmIDRegistrationEvent
 * <p>
 * Handler receives from all other channels:
 * - SwarmIDRegisteredAcknowledgementEvent
 * <p>
 * Handler listens to:
 * - SwarmIDRegistrationEvent
 * - SwarmIDRegistrationAcknowledgementEvent
 * - UpdateNeighboursEvent
 *
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 22.05.15
 */
public final class SwarmIDRegistrationHandler extends ChannelHandlerAdapter {

    public static final long UPDATE_INTERVAL_MS = 500;

    // The other, which use the same swarm id manager
    private final ChannelGroup channels;

    // The swarm id manager
    private final SwarmIDManager swarmIDManager;

    // The channel handler context
    private ChannelHandlerContext ctx;

    // The current update neighbours message
    private UpdateNeighboursMessage updateNeighboursMessage = new UpdateNeighboursMessage();

    // The update future is set, when neighbours get updated
    private ChannelPromise updateChannelPromise;

    // The local swarm id
    private SwarmID localSwarmID;

    private void fireSwarmIDAcquired() {
        ctx.pipeline().fireUserEventTriggered(new SwarmIDAcquisitionEvent(localSwarmID));
    }

    private void fireUpdateNeighbours() {
        ctx.pipeline().fireUserEventTriggered(new UpdateNeighboursEvent());
    }

    private void scheduleFireUpdateNeighbours() {
        ctx.executor().schedule(this::fireUpdateNeighbours, UPDATE_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    private void broadcastSwarmIDRegistered() {
        SwarmIDRegistrationEvent reg = new SwarmIDRegistrationEvent(this.ctx.channel(),
                                                                    localSwarmID,
                                                                    SwarmIDRegistrationEvent.Type.Registered);
        // Send to parent and remaining channels
        ctx.channel().parent().pipeline().fireUserEventTriggered(reg);
        channels.forEach(c -> c.pipeline().fireUserEventTriggered(reg));
    }

    private void broadcastSwarmIDUnregistered() {
        SwarmIDRegistrationEvent unreg = new SwarmIDRegistrationEvent(this.ctx.channel(),
                                                                      localSwarmID,
                                                                      SwarmIDRegistrationEvent.Type.Unregistered);
        // Send to parent and remaining channels
        ctx.channel().parent().pipeline().fireUserEventTriggered(unreg);
        channels.forEach(c -> c.pipeline().fireUserEventTriggered(unreg));
    }

    private void replySwarmIDAcknowledged(SwarmIDRegistrationEvent evt) {
        SwarmIDRegistrationAcknowledgementEvent ack = new SwarmIDRegistrationAcknowledgementEvent(this.ctx.channel(),
                                                                                                  localSwarmID);
        evt.getChannel().pipeline().fireUserEventTriggered(ack);
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
        SwarmID newLocalSwarmID = swarmIDManager.register(new InetSocketAddress(inetSocketAddress.getAddress(), port));

        // Set the local swarm id
        localSwarmID = newLocalSwarmID;

        // Send the new local swarm id to the remote
        ctx.writeAndFlush(new SetLocalSwarmIDMessage(newLocalSwarmID))
           .addListener(fut -> {
               if (fut.isSuccess()) {
                   // Let handler chain know, that we have acquired our swarm id
                   fireSwarmIDAcquired();
                   broadcastSwarmIDRegistered();
               }
           })
           .addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
    }

    private void handleSwarmIDRegistrationAcknowledgementEvent(SwarmIDRegistrationAcknowledgementEvent evt) {
        updateNeighboursMessage.getAddNeighbours().add(evt.getSwarmID());
    }

    private void handleSwarmIDRegistrationEvent(SwarmIDRegistrationEvent evt) {
        switch (evt.getType()) {
            case Registered:
                updateNeighboursMessage.getAddNeighbours().add(evt.getSwarmID());
                replySwarmIDAcknowledged(evt);
                break;
            case Unregistered:
                updateNeighboursMessage.getAddNeighbours().remove(evt.getSwarmID());
                updateNeighboursMessage.getRemoveNeighbours().add(evt.getSwarmID());
                break;
        }
    }

    private void handleUpdateNeighboursEvent(UpdateNeighboursEvent evt) {
        updateNeighbours();
        scheduleFireUpdateNeighbours();
    }

    private void updateNeighbours() {

        // We are not ready to participate yet
        if (localSwarmID == null) {
            return;
        }

        // No updates, skip this update
        if (updateNeighboursMessage.isEmpty()) {
            return;
        }

        // Make sure, the message is valid
        if (!updateNeighboursMessage.isDistinct()) {
            // Send to channel
            ctx.channel()
               .pipeline()
               .fireExceptionCaught(new IllegalStateException("!updateNeighboursMessage.isDistinct()"));
            return;
        }

        // Pending update channel future, just ignore the current update
        if (updateChannelPromise != null) {
            return;
        }

        // Prepare promise
        updateChannelPromise = ctx.newPromise();

        // Write the update neighbours message
        ctx.writeAndFlush(updateNeighboursMessage, updateChannelPromise)
           .addListener(fut -> updateChannelPromise = null)
           .addListener(ChannelFutureListener.CLOSE_ON_FAILURE);

        // Create a new update message
        updateNeighboursMessage = new UpdateNeighboursMessage();
    }

    public SwarmIDRegistrationHandler(ChannelGroup channels, SwarmIDManager swarmIDManager) {
        Objects.requireNonNull(channels);
        Objects.requireNonNull(swarmIDManager);
        this.channels = channels;
        this.swarmIDManager = swarmIDManager;
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof SwarmIDRegistrationEvent) {
            // We are not ready to participate yet
            if (localSwarmID == null) {
                return;
            }

            // Cast
            SwarmIDRegistrationEvent swarmIDRegistrationEvent = (SwarmIDRegistrationEvent) evt;

            // Ignore ping-back passages
            if (localSwarmID.equals(swarmIDRegistrationEvent.getSwarmID())) {
                return;
            }

            // Handle the registration event
            handleSwarmIDRegistrationEvent((SwarmIDRegistrationEvent) evt);

            super.userEventTriggered(ctx, evt);
        }
        if (evt instanceof SwarmIDRegistrationAcknowledgementEvent) {
            handleSwarmIDRegistrationAcknowledgementEvent((SwarmIDRegistrationAcknowledgementEvent) evt);
        }
        if (evt instanceof UpdateNeighboursEvent) {
            handleUpdateNeighboursEvent((UpdateNeighboursEvent) evt);
        } else {
            super.userEventTriggered(ctx, evt);
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        this.ctx = ctx;
        scheduleFireUpdateNeighbours();
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if (localSwarmID != null) {
            swarmIDManager.unregister(localSwarmID);
            broadcastSwarmIDUnregistered();
        }
        super.channelInactive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        boolean hasNotLocalSwarmID = localSwarmID == null;
        boolean isSetCollectorAddressMessage = msg instanceof SetCollectorAddressMessage;

        if (hasNotLocalSwarmID || isSetCollectorAddressMessage) {
            if (!hasNotLocalSwarmID || !isSetCollectorAddressMessage) {
                throw new IllegalStateException("!hasNotLocalSwarmID || !isSetCollectorAddressMessage");
            } else {
                setCollectorAddress(ctx, (SetCollectorAddressMessage) msg);
            }
        } else {
            super.channelRead(ctx, msg);
        }
    }
}
