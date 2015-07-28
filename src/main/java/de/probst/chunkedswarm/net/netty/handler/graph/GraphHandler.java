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
        // Create a graph to compute the meshes
        Graph<String> graph = new Graph<>();

        // The master node group
        NodeGroup<String> masterNodeGroup = new NodeGroup<>();

        // Create a node group for each node
        acknowledgedNeighbours.values().forEach(evt -> {
            NodeGroup<String> nodeGroup = new NodeGroup<>();

            // Add all acknowledged neighbours into group
            nodeGroup.getNodes().addAll(evt.getAcknowledgedOutboundNeighbours());

            // Of course, each node is connected with the master
            nodeGroup.getNodes().add(masterUuid);

            // The master is connected with this node
            masterNodeGroup.getNodes().add(evt.getLocalSwarmId().getUuid());

            // Put node group into graph
            graph.getNodes().put(evt.getLocalSwarmId().getUuid(), nodeGroup);
        });

        // Put the view of the master into the graph
        graph.getNodes().put(masterUuid, masterNodeGroup);

        // Compute the meshes
        return graph.findMeshes(masterUuid);
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
