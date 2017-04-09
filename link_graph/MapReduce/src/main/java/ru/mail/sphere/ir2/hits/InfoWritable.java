package ru.mail.sphere.ir2.hits;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class InfoWritable implements Writable {
    private int nodeFromIndex;
    private long authority, hub;

    InfoWritable() {
        authority = 1;
        hub = 1;
        nodeFromIndex = -1;
    }

    InfoWritable(int nodeFromIndex, long authority, long hub) {
        this.nodeFromIndex = nodeFromIndex;
        this.authority = authority;
        this.hub = hub;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(nodeFromIndex);
        out.writeLong(authority);
        out.writeLong(hub);
    }

    public void readFields(DataInput in) throws IOException {
        nodeFromIndex = in.readInt();
        authority = in.readLong();
        hub = in.readLong();
    }

    public long getAuthority() {
        return authority;
    }

    public long getHub() {
        return hub;
    }

    public int getNodeFromIndex() {
        return nodeFromIndex;
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(nodeFromIndex);
        stringBuilder.append(" ");
        stringBuilder.append(authority);
        stringBuilder.append(" ");
        stringBuilder.append(hub);
        return stringBuilder.toString();
    }
}
