package ru.mail.sphere.ir2.hits;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class NodeWritable implements Writable {
    private long authority, hub;
    private List<Integer> adjacencyList;

    public NodeWritable() {
        authority = 1;
        hub = 1;
        adjacencyList = new LinkedList<Integer>();
    }

    public NodeWritable(long authority, long hub, List<Integer> adjacencyList) {
        this.authority = authority;
        this.hub = hub;
        this.adjacencyList = new LinkedList<Integer>(adjacencyList);
    }

    public void write(DataOutput out) throws IOException {
        out.writeLong(authority);
        out.writeLong(hub);

        out.writeInt(adjacencyList.size());
        for (Integer nodeIndex: adjacencyList) {
            out.writeInt(nodeIndex);
        }
    }

    public void readFields(DataInput in) throws IOException {
        authority = in.readLong();
        hub = in.readLong();

        int adjacencyListSize = in.readInt();
        for (; adjacencyListSize > 0; adjacencyListSize--) {
            adjacencyList.add(in.readInt());
        }
    }

    public long getAuthority() {
        return authority;
    }

    public long getHub() {
        return hub;
    }

    public List<Integer> getAdjacencyList() {
        return adjacencyList;
    }

    public int getAdjacencyListSize() {
        return adjacencyList.size();
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();

        stringBuilder.append(authority);
        stringBuilder.append(" ");
        stringBuilder.append(hub);
        stringBuilder.append(" ");
        for (Integer index : adjacencyList) {
            stringBuilder.append(index);
            stringBuilder.append(" ");
        }

        return stringBuilder.toString();
    }
}
