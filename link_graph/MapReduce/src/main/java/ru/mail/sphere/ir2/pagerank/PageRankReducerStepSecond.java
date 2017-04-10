package ru.mail.sphere.ir2.pagerank;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PageRankReducerStepSecond extends Reducer<LongWritable, NodeWritable, LongWritable, NodeWritable> {
    @Override
    protected void reduce(LongWritable key, Iterable<NodeWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key, values.iterator().next());
    }
}
