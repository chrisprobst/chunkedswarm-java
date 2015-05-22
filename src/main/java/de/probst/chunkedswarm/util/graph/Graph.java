package de.probst.chunkedswarm.util.graph;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.ListIterator;
import java.util.Map;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 21.03.15
 */
public final class Graph<T> implements Cloneable {

    // A nodes is just a set of nodes mapped to their neighbours
    private final Map<T, NodeGroup<T>> nodes = new HashMap<>();

    private void removeUnidirectionalLinks() {
        // Ignore correct reverse neighbours
        nodes.entrySet().forEach(entry -> entry.getValue()
                                               .getNodes()
                                               .removeIf(n -> !nodes.get(n).getNodes().contains(entry.getKey())));

    }

    private void insertCandidate(NodeGroups<T> meshCandidates, NodeGroup<T> newMeshCandidate) {
        for (ListIterator<NodeGroup<T>> i = meshCandidates.getGroups().listIterator(); i.hasNext(); ) {
            NodeGroup<T> meshCandidate = i.next();

            if (newMeshCandidate.isSuperGroupOf(meshCandidate)) {
                // Ignore super groups, they are not specific enough
                return;
            } else if (meshCandidate.isSuperGroupOf(newMeshCandidate)) {
                // Replace old broader candidate with new candidate
                i.set(newMeshCandidate);
                return;
            }
        }

        // No super groups, just add =)
        meshCandidates.getGroups().add(newMeshCandidate);
    }

    public Map<T, NodeGroup<T>> getNodes() {
        return nodes;
    }

    @Override
    public Graph<T> clone() {
        Graph<T> clone = new Graph<>();
        nodes.entrySet().forEach(entry -> {
            NodeGroup<T> ng = new NodeGroup<>();
            ng.getNodes().addAll(entry.getValue().getNodes());
            clone.getNodes().put(entry.getKey(), ng);
        });
        return clone;
    }

    public NodeGroups<T> findMeshes(T root) {
        // Lookup the neighbours
        NodeGroup<T> revNeighbours = nodes.get(root);
        if (revNeighbours == null) {
            throw new IllegalArgumentException("Root does not exist");
        }

        // Make sure, the nodes has no unidirectional links
        removeUnidirectionalLinks();

        // Here we store all mesh candidates
        NodeGroups<T> meshCandidates = new NodeGroups<>();

        // Iterate over all reverse neighbours
        revNeighbours.getNodes().forEach(revNeighbour -> {
            // The next mesh candidate is the intersection
            NodeGroup<T> meshCandidate = nodes.get(revNeighbour).intersection(revNeighbours);

            // Add root and reverse neighbour
            meshCandidate.getNodes().add(root);
            meshCandidate.getNodes().add(revNeighbour);

            // Insert the mesh candidate, if valid!
            insertCandidate(meshCandidates, meshCandidate);
        });

        // The final result should not contain the root!
        meshCandidates.getGroups().forEach(ng -> ng.getNodes().remove(root));

        // Sort according to the number of nodes per group (highest first)
        meshCandidates.getGroups().sort(Comparator.reverseOrder());

        return meshCandidates;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Graph<?> graph = (Graph<?>) o;

        return nodes.equals(graph.nodes);

    }

    @Override
    public int hashCode() {
        return nodes.hashCode();
    }

    @Override
    public String toString() {
        return "Graph{" +
               "nodes=" + nodes +
               '}';
    }

    public static void main(String[] args) {
        Graph<Integer> g = new Graph<>();
        NodeGroup<Integer> expected = new NodeGroup<>();
        int count = 100;
        for (int i = 0; i < count; i++) {
            if (i != 0) {
                expected.getNodes().add(i);
            }

            NodeGroup<Integer> neighbours = new NodeGroup<>();
            for (int j = 0; j < count; j++) {
                if (i != j) {
                    neighbours.getNodes().add(j);
                }
            }

            g.getNodes().put(i, neighbours);
        }

        // Find all meshes of the nodes
        NodeGroups<Integer> meshes = g.findMeshes(0);

        System.out.println(meshes.equals(new NodeGroups<>(Arrays.asList(expected))));
    }
}