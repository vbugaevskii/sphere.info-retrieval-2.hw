package ru.mail.sphere.ir2.hits;

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

public class HITSResults extends Configured implements Tool {
    public static class ResultsMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] pair = value.toString().split("\t");
            if (pair.length < 2) {
                return;
            }

            String[] values = pair[1].split(" ");

            LongWritable node = new LongWritable(Integer.valueOf(pair[0]));
            LongWritable authority = new LongWritable(Long.valueOf(values[0]));
            // LongWritable hub = new LongWritable(Long.valueOf(values[1]));

            context.write(node, authority);
            // context.write(node, hub);
        }
    }

    public static class ResultsReducer extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
        private final static int TOP_SIZE = 30;
        private Map<Long, Long> nodes = new HashMap<Long, Long>();

        @Override
        protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            if (nodes.size() < TOP_SIZE) {
                nodes.put(key.get(), values.iterator().next().get());
            } else {
                long candidate = -1;
                long rank = values.iterator().next().get();
                long rankMin = rank;

                for (Map.Entry<Long, Long> pair : nodes.entrySet()) {
                    long rankCurr = pair.getValue();
                    if (rankCurr < rank && rankCurr < rankMin) {
                        candidate = pair.getKey();
                        rankMin = rankCurr;
                    }
                }

                if (candidate > 0) {
                    nodes.remove(candidate);
                    nodes.put(key.get(), rank);
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            List<Map.Entry<Long, Long>> orderedMap =
                    new LinkedList<Map.Entry<Long, Long>>(nodes.entrySet());

            Collections.sort(orderedMap, new Comparator<Map.Entry<Long, Long>>()
            {
                public int compare(Map.Entry<Long, Long> o1, Map.Entry<Long, Long> o2) {
                    return -(o1.getValue()).compareTo(o2.getValue());
                }
            });

            for (Map.Entry<Long, Long> pair : orderedMap) {
                context.write(new LongWritable(pair.getKey()), new LongWritable(pair.getValue()));
            }

            super.cleanup(context);
        }
    }

    private Job getJobConf(String input, String output) throws IOException {
        Job job = Job.getInstance(getConf());

        job.setJarByClass(HITSResults.class);
        job.setJobName("HITS. Result");

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setMapperClass(HITSResults.ResultsMapper.class);
        job.setReducerClass(HITSResults.ResultsReducer.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setNumReduceTasks(1);
        return job;
    }

    public int run(String[] args) throws Exception {
        Job job = getJobConf(args[1], args[2]);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    static public void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new HITSResults(), args);
        if (ret > 0) {
            System.exit(ret);
        } else {
            Denumerator denumerator = new Denumerator(new Path(args[0]));
            denumerator.map(new Path(args[2] + "/part-r-00000"));
        }
    }
}
