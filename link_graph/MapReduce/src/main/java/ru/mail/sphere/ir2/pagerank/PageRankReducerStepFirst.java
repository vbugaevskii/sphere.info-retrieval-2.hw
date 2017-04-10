package ru.mail.sphere.ir2.pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class PageRankReducerStepFirst extends Reducer<LongWritable, NodeWritable, LongWritable, NodeWritable> {
    private float probabilityLeft = 0.0f;

    @Override
    protected void reduce(LongWritable key, Iterable<NodeWritable> values, Context context)
            throws IOException, InterruptedException {
        float probability = 0.0f;
        List<Integer> nodesTo = new LinkedList<Integer>();

        for (NodeWritable node : values) {
            if (node.getProbability() < 0.0f) {
                nodesTo.addAll(node.getAdjacencyList());
            } else {
                probability += node.getProbability();
            }
        }

        if (nodesTo.isEmpty()) {
            probabilityLeft += probability;
        }

        context.write(key, new NodeWritable(probability, nodesTo));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.getConfiguration().setFloat(PageRankJob.parameterLeftRank, probabilityLeft);

        super.cleanup(context);
    }
}
