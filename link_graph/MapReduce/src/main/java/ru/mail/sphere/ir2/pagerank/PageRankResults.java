package ru.mail.sphere.ir2.pagerank;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import ru.mail.sphere.ir2.numerator.Denumerator;

import java.io.IOException;
import java.util.*;

public class PageRankResults extends Configured implements Tool {
    public static class ResultsMapper extends Mapper<LongWritable, Text, LongWritable, FloatWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] pair = value.toString().split("\t");
            if (pair.length < 2) {
                return;
            }

            String[] values = pair[1].split(" ");

            LongWritable node = new LongWritable(Integer.valueOf(pair[0]));
            FloatWritable probability = new FloatWritable(Float.valueOf(values[0]));

            context.write(node, probability);
        }
    }

    public static class ResultsReducer extends Reducer<LongWritable, FloatWritable, LongWritable, FloatWritable> {
        private final static int TOP_SIZE = 30;
        private Map<Long, Float> nodes = new HashMap<Long, Float>();

        @Override
        protected void reduce(LongWritable key, Iterable<FloatWritable> values, Context context)
                throws IOException, InterruptedException {
            if (nodes.size() < TOP_SIZE) {
                nodes.put(key.get(), values.iterator().next().get());
            } else {
                long  candidate = -1;
                float prob = values.iterator().next().get();
                float probMin = 1.0f;

                for (Map.Entry<Long, Float> pair : nodes.entrySet()) {
                    float probCurr = pair.getValue();
                    if (probCurr < prob && probCurr < probMin) {
                        candidate = pair.getKey();
                        probMin = probCurr;
                    }
                }

                if (candidate > 0) {
                    nodes.remove(candidate);
                    nodes.put(key.get(), prob);
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            List<Map.Entry<Long, Float>> orderedMap =
                    new LinkedList<Map.Entry<Long, Float>>(nodes.entrySet());

            Collections.sort(orderedMap, new Comparator<Map.Entry<Long, Float>>()
            {
                public int compare(Map.Entry<Long, Float> o1, Map.Entry<Long, Float> o2) {
                    return -(o1.getValue()).compareTo(o2.getValue());
                }
            });

            for (Map.Entry<Long, Float> pair : orderedMap) {
                context.write(new LongWritable(pair.getKey()), new FloatWritable(pair.getValue()));
            }

            super.cleanup(context);
        }
    }

    private Job getJobConf(String input, String output) throws IOException {
        Job job = Job.getInstance(getConf());

        job.setJarByClass(PageRankResults.class);
        job.setJobName("PageRank. Result");

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setMapperClass(ResultsMapper.class);
        job.setReducerClass(ResultsReducer.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(FloatWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setNumReduceTasks(1);
        return job;
    }

    public int run(String[] args) throws Exception {
        Job job = getJobConf(args[1], args[2]);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    static public void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new PageRankResults(), args);
        if (ret > 0) {
            System.exit(ret);
        } else {
            Denumerator denumerator = new Denumerator(new Path(args[0]));
            denumerator.map(new Path(args[2] + "/part-r-00000"));
        }
    }
}