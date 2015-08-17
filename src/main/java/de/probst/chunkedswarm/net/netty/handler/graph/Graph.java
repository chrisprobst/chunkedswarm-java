package de.probst.chunkedswarm.net.netty.handler.graph;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 21.03.15
 */
public final class Graph<T> implements Cloneable {

    private static <T> void insertCandidate(NodeGroups<T> meshCandidates, NodeGroup<T> newMeshCandidate) {
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

    // A nodes is just a set of nodes mapped to their neighbours
    private Map<T, NodeGroup<T>> nodes = new HashMap<>();

    private void removeUnidirectionalLinks() {
        // Ignore correct reverse neighbours
        nodes.entrySet().forEach(entry -> entry.getValue()
                                               .getNodes()
                                               .removeIf(n -> !nodes.containsKey(n) ||
                                                              !nodes.get(n).getNodes().contains(entry.getKey())));

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

    public NodeGroups<T> findMeshes(T root, Graph<T> intersectionGraph) {
        Objects.requireNonNull(root);
        Objects.requireNonNull(intersectionGraph);

        // Make sure to use copies
        Graph<T> graphClone = clone();
        Graph<T> intersectionGraphClone = intersectionGraph.clone();

        // Lookup the neighbours
        NodeGroup<T> revNeighbours = graphClone.nodes.get(root);
        if (revNeighbours == null) {
            throw new IllegalArgumentException("Root does not exist");
        }

        // Lookup the neighbours in the intersection graph
        NodeGroup<T> revIntersectionNeighbours = intersectionGraphClone.nodes.get(root);
        if (revIntersectionNeighbours == null) {
            throw new IllegalArgumentException("Intersection root does not exist");
        }

        // Make intersection
        NodeGroup<T> revNeighboursIntersection = revNeighbours.intersection(revIntersectionNeighbours);

        // Make sure, the graphs have no unidirectional links
        graphClone.removeUnidirectionalLinks();
        intersectionGraphClone.removeUnidirectionalLinks();

        // Here we store all mesh candidates
        NodeGroups<T> meshCandidates = new NodeGroups<>();

        // Iterate over all reverse neighbours in the intersection
        revNeighboursIntersection.getNodes().forEach(revNeighbour -> {

            // The next mesh candidate
            NodeGroup<T> meshCandidate = graphClone.nodes.get(revNeighbour).intersection(revNeighboursIntersection);

            // The next intersection mesh candidate
            NodeGroup<T> intersectionMeshCandidate = intersectionGraphClone.nodes.get(revNeighbour).intersection(
                    revNeighboursIntersection);

            // Compute the intersection of both
            NodeGroup<T> meshCandidateIntersection = meshCandidate.intersection(intersectionMeshCandidate);

            // Add root and reverse neighbour
            meshCandidateIntersection.getNodes().add(root);
            meshCandidateIntersection.getNodes().add(revNeighbour);

            // Insert the mesh candidate, if valid!
            insertCandidate(meshCandidates, meshCandidateIntersection);
        });

        // The final result should not contain the root!
        meshCandidates.getGroups().forEach(ng -> ng.getNodes().remove(root));

        // Sort according to the number of nodes per group (highest first)
        meshCandidates.getGroups().sort(Comparator.reverseOrder());

        return meshCandidates;
    }

    public NodeGroups<T> findMeshes(T root) {
        return findMeshes(root, this);
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
        Graph<Integer> g2 = new Graph<>();
        int count = 100;

        // Create g links
        for (int i = 0; i < count; i++) {
            NodeGroup<Integer> neighbours = new NodeGroup<>();
            for (int j = 0; j < count; j++) {
                if (i != j) {
                    neighbours.getNodes().add(j);
                }
            }

            g.getNodes().put(i, neighbours);
        }

        // Create g2 links
        for (int i = 0; i < count; i += 2) {
            NodeGroup<Integer> neighbours = new NodeGroup<>();
            for (int j = 0; j < count; j += 2) {
                if (i != j) {
                    neighbours.getNodes().add(j);
                }
            }

            g2.getNodes().put(i, neighbours);
        }

        // Create expected values
        NodeGroup<Integer> expected = new NodeGroup<>();
        for (int i = 0; i < count; i += 2) {
            if (i != 0) {
                expected.getNodes().add(i);
            }
        }

        // Find all meshes of the nodes
        NodeGroups<Integer> meshes = g.findMeshes(0, g2);

        System.out.println(meshes.equals(new NodeGroups<>(Collections.singletonList(expected))));
    }
}