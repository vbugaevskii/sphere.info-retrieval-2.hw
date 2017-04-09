package ru.mail.sphere.ir2.pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class PageRankMapperInit extends Mapper<LongWritable, Text, LongWritable, NodeWritable> {
    private int indexTotal = 1147103;
    private float probability = 1.0f;
    private static List<Integer> emptyList = new LinkedList<Integer>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        Configuration config = context.getConfiguration();
        indexTotal = config.getInt("total", indexTotal);
        probability = 1.0f / indexTotal;
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] pair = value.toString().split("\t");
        if (pair.length < 2) {
            return;
        }

        int nodeFromIndex = Integer.valueOf(pair[0]);
        String[] adjacencyList = pair[1].split(" ");
        List<Integer> nodesToIndex = new LinkedList<Integer>();

        for (String nodeIndex : adjacencyList) {
            nodesToIndex.add(Integer.valueOf(nodeIndex));
        }

        context.write(new LongWritable(nodeFromIndex), new NodeWritable(probability, nodesToIndex));
        for (Integer nodeIndex : nodesToIndex) {
            context.write(new LongWritable(nodeIndex), new NodeWritable(probability, emptyList));
        }
    }
}
