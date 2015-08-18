package de.probst.chunkedswarm.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 21.03.15
 */
public class NodeGroups<T> {

    private final List<NodeGroup<T>> nodeGroups;

    public NodeGroups(List<NodeGroup<T>> nodeGroups) {
        Objects.requireNonNull(nodeGroups);
        this.nodeGroups = nodeGroups;
    }

    public NodeGroups() {
        this(new ArrayList<>());
    }

    public List<NodeGroup<T>> getGroups() {
        return nodeGroups;
    }

    public boolean hasSuperGroupOf(NodeGroup<?> o) {
        return nodeGroups.stream().anyMatch(ng -> ng.isSuperGroupOf(o));
    }

    public boolean hasSubGroupOf(NodeGroup<?> o) {
        return nodeGroups.stream().anyMatch(o::isSuperGroupOf);
    }

    public boolean hasStrictSuperGroupOf(NodeGroup<?> o) {
        return nodeGroups.stream().anyMatch(ng -> ng.isStrictSuperGroupOf(o));
    }

    public boolean hasStrictSubGroupOf(NodeGroup<?> o) {
        return nodeGroups.stream().anyMatch(o::isStrictSuperGroupOf);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        NodeGroups<?> that = (NodeGroups<?>) o;

        return nodeGroups.equals(that.nodeGroups);

    }

    @Override
    public int hashCode() {
        return nodeGroups.hashCode();
    }

    @Override
    public String toString() {
        return "NodeGroups{" +
               "nodeGroups=" + nodeGroups +
               '}';
    }
}