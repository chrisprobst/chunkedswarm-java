package de.probst.chunkedswarm.util;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 21.03.15
 */
public class NodeGroup<T> {

    // All nodes
    private final Set<T> nodes;

    public NodeGroup(Set<T> nodes) {
        Objects.requireNonNull(nodes);
        this.nodes = nodes;
    }

    public NodeGroup() {
        this(new HashSet<>());
    }

    public Set<T> getNodes() {
        return nodes;
    }

    public boolean isSuperGroupOf(NodeGroup<?> o) {
        return nodes.containsAll(o.getNodes());
    }

    public boolean isStrictSuperGroupOf(NodeGroup<?> o) {
        return isSuperGroupOf(o) && !o.isSuperGroupOf(this);
    }

    public NodeGroup<T> intersection(NodeGroup<?> o) {
        NodeGroup<T> t = new NodeGroup<>(new HashSet<>(nodes));
        t.nodes.retainAll(o.nodes);
        return t;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        NodeGroup<?> nodeGroup = (NodeGroup<?>) o;

        return nodes.equals(nodeGroup.nodes);

    }

    @Override
    public int hashCode() {
        return nodes.hashCode();
    }

    @Override
    public String toString() {
        return "NodeGroup{" +
               "nodes=" + nodes +
               '}';
    }
}