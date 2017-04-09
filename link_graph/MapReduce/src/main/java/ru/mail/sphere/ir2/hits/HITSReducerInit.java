package ru.mail.sphere.ir2.hits;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class HITSReducerInit extends Reducer<LongWritable, InfoWritable, LongWritable, NodeWritable> {
    @Override
    protected void reduce(LongWritable key, Iterable<InfoWritable> values, Context context)
            throws IOException, InterruptedException {
        long authority = 1;
        long hub = 1;

        List<Integer> nodesFromHub = new LinkedList<Integer>();
        List<Integer> nodesFromAuthority = new LinkedList<Integer>();

        for (InfoWritable node : values) {
            if (node.getAuthority() > 0) {
                nodesFromAuthority.add(node.getNodeFromIndex());
                hub += node.getAuthority();
            } else {
                nodesFromHub.add(node.getNodeFromIndex());
                authority += node.getHub();
            }
        }

        context.write(key, new NodeWritable(0, hub, nodesFromAuthority));
        context.write(key, new NodeWritable(authority, 0, nodesFromHub));
    }
}
