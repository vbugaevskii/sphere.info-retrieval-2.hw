package ru.mail.sphere.ir2.pagerank;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class PageRankReducerInit extends Reducer<LongWritable, NodeWritable, LongWritable, NodeWritable> {
    @Override
    protected void reduce(LongWritable key, Iterable<NodeWritable> values, Context context)
            throws IOException, InterruptedException {
        float probability = 0.0f;
        List<Integer> nodesTo = new LinkedList<Integer>();

        for (NodeWritable node : values) {
            if (node.getAdjacencyListSize() > 0) {
                nodesTo.addAll(node.getAdjacencyList());
            } else {
                probability = node.getProbability();
            }
        }

        context.write(key, new NodeWritable(probability, nodesTo));
    }
}