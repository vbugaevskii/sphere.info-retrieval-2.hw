package ru.mail.sphere.ir2.pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class PageRankReducer extends Reducer<LongWritable, NodeWritable, LongWritable, NodeWritable> {
    private float alpha = 0.8f;
    private int indexTotal = 1147103;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        Configuration config = context.getConfiguration();
        indexTotal = config.getInt("total", indexTotal);
        alpha = config.getFloat("alpha", alpha);
    }

    @Override
    protected void reduce(LongWritable key, Iterable<NodeWritable> values, Context context)
            throws IOException, InterruptedException {
        float probability = 0.0f;
        List<Integer> nodesTo = new LinkedList<Integer>();

        for (NodeWritable node : values) {
            if (node.getAdjacencyListSize() > 0) {
                nodesTo.addAll(node.getAdjacencyList());
            } else {
                probability += node.getProbability();
            }
        }

        probability = alpha * probability + (1.0f - alpha) / indexTotal;
        context.write(key, new NodeWritable(probability, nodesTo));
    }
}
