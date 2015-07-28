package de.probst.chunkedswarm.net.netty.handler.graph;

import de.probst.chunkedswarm.net.netty.handler.connection.event.AcknowledgedNeighboursEvent;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 02.06.15
 */
public final class GraphHandler extends ChannelHandlerAdapter {

    // The master uuid, so nobody can choose this id
    private final String masterUuid;

    // Used to store all incoming events
    private final Map<String, AcknowledgedNeighboursEvent> acknowledgedNeighbours = new HashMap<>();

    private NodeGroups<String> computeMeshes() {
        // Create graphs to compute the meshes
        Graph<String> outboundGraph = new Graph<>();
        Graph<String> inboundGraph = new Graph<>();

        // The master node groups
        NodeGroup<String> masterOutboundNodeGroup = new NodeGroup<>();
        NodeGroup<String> masterInboundNodeGroup = new NodeGroup<>();

        // Create node groups for each node
        acknowledgedNeighbours.values().forEach(evt -> {
            NodeGroup<String> outboundNodeGroup = new NodeGroup<>();
            NodeGroup<String> inboundNodeGroup = new NodeGroup<>();

            // Add all acknowledged neighbours into group
            outboundNodeGroup.getNodes().addAll(evt.getAcknowledgedOutboundNeighbours());
            inboundNodeGroup.getNodes().addAll(evt.getAcknowledgedInboundNeighbours());

            // Of course, each node is connected with the master
            outboundNodeGroup.getNodes().add(masterUuid);
            inboundNodeGroup.getNodes().add(masterUuid);

            // The master is connected with this node
            masterOutboundNodeGroup.getNodes().add(evt.getLocalSwarmId().getUuid());
            masterInboundNodeGroup.getNodes().add(evt.getLocalSwarmId().getUuid());

            // Put node groups into graphs
            outboundGraph.getNodes().put(evt.getLocalSwarmId().getUuid(), outboundNodeGroup);
            inboundGraph.getNodes().put(evt.getLocalSwarmId().getUuid(), inboundNodeGroup);
        });

        // Put the view of the master into the graphs
        outboundGraph.getNodes().put(masterUuid, masterOutboundNodeGroup);
        inboundGraph.getNodes().put(masterUuid, masterInboundNodeGroup);

        // Compute the meshes
        return outboundGraph.findMeshes(masterUuid, inboundGraph);
    }

    private void handleAcknowledgedNeighboursEvent(AcknowledgedNeighboursEvent evt) {
        switch (evt.getType()) {
            case Update:
                acknowledgedNeighbours.put(evt.getLocalSwarmId().getUuid(), evt);
                break;
            case Dispose:
                acknowledgedNeighbours.remove(evt.getLocalSwarmId().getUuid());
                break;
        }


        NodeGroups<String> meshes = computeMeshes();
        if (!meshes.getGroups().isEmpty()) {
            System.out.println("Node group size: " + meshes.getGroups().get(0).getNodes().size());
        }
    }

    public GraphHandler(String masterUuid) {
        Objects.requireNonNull(masterUuid);
        this.masterUuid = masterUuid;
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof AcknowledgedNeighboursEvent) {
            handleAcknowledgedNeighboursEvent((AcknowledgedNeighboursEvent) evt);
        }

        super.userEventTriggered(ctx, evt);
    }
}
