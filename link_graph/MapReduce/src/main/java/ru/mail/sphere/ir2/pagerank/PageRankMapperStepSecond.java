package ru.mail.sphere.ir2.pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class PageRankMapperStepSecond extends Mapper<LongWritable, Text, LongWritable, NodeWritable> {
    private float probabilityLeft;
    private float alpha;
    private int indexTotal;

    private static List<Integer> emptyList = new LinkedList<Integer>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        Configuration config = context.getConfiguration();
        indexTotal = config.getInt(PageRankJob.parameterN, PageRankJob.DEFAULT_N);
        alpha = config.getFloat(PageRankJob.parameterAlpha, PageRankJob.DEFAULT_ALPHA);
        probabilityLeft = config.getFloat(PageRankJob.parameterLeftRank, 0.0f);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] pair = value.toString().split("\t");
        if (pair.length < 2) {
            return;
        }

        int nodeFromIndex = Integer.valueOf(pair[0]);
        String[] values = pair[1].split(" ");
        float probability = Float.valueOf(values[0]);
        List<Integer> adjacencyList;

        if (values.length > 1) {
            adjacencyList = new LinkedList<Integer>();
            for (int i = 1; i < values.length; i++) {
                adjacencyList.add(Integer.valueOf(values[i]));
            }
        } else {
            adjacencyList = emptyList;
        }

        probability = alpha * (probability + probabilityLeft / indexTotal) + (1.0f - alpha) / indexTotal;
        context.write(new LongWritable(nodeFromIndex), new NodeWritable(probability, adjacencyList));
    }
}
