package ru.mail.sphere.ir2.parser;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class ParserJob extends Configured implements Tool {
    private Job getJobConf(String input, String output) throws IOException {
        Job job = Job.getInstance(getConf());

        job.setJarByClass(ParserJob.class);
        job.setJobName("Parser");

        TextInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setMapperClass(ParserMapper.class);
        job.setReducerClass(ParserReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TextListWritable.class);

        job.setNumReduceTasks(1);
        return job;
    }

    public int run(String[] args) throws Exception {
        Job job = getJobConf(args[0], args[1]);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    static public void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new ParserJob(), args);
        System.exit(ret);
    }
}