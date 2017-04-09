package ru.mail.sphere.ir2.hits;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class HITSMapper extends Mapper<LongWritable, Text, LongWritable, InfoWritable> {
    private static List<Integer> emptyList = new LinkedList<Integer>();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] pair = value.toString().split("\t");
        if (pair.length < 2) {
            return;
        }

        int nodeIndexFrom = Integer.valueOf(pair[0]);
        String[] values = pair[1].split(" ");
        long authority = Long.valueOf(values[0]);
        long hub = Long.valueOf(values[1]);
        List<Integer> adjacencyList;

        if (values.length > 2) {
            adjacencyList = new LinkedList<Integer>();
            for (int i = 2; i < values.length; i++) {
                adjacencyList.add(Integer.valueOf(values[i]));
            }
        } else {
            adjacencyList = emptyList;
        }

        for (Integer nodeIndexTo: adjacencyList) {
            context.write(new LongWritable(nodeIndexTo), new InfoWritable(nodeIndexFrom, authority, hub));
        }
    }
}
