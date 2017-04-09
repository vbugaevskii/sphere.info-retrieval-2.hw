package ru.mail.sphere.ir2.hits;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class HITSMapperInit extends Mapper<LongWritable, Text, LongWritable, InfoWritable> {
    private static List<Integer> emptyList = new LinkedList<Integer>();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] pair = value.toString().split("\t");
        if (pair.length < 2) {
            return;
        }

        int nodeIndexFrom = Integer.valueOf(pair[0]);
        String[] values = pair[1].split(" ");

        for (String node: values) {
            int nodeIndexTo = Integer.valueOf(node);
            context.write(new LongWritable(nodeIndexTo), new InfoWritable(nodeIndexFrom, 0, 1));
            context.write(new LongWritable(nodeIndexFrom), new InfoWritable(nodeIndexTo, 1, 0));
        }
    }
}
