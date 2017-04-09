package ru.mail.sphere.ir2.pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.*;

public class PageRankJob extends Configured implements Tool {
    private int iterationCount = 30;

    private Job getJobConf(String input, String output, int currentIteration) throws IOException {
        Job job = Job.getInstance(getConf());

        job.setJarByClass(PageRankJob.class);
        job.setJobName(String.format("PageRank. Iteration=%02d", currentIteration));

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        if (currentIteration > 0) {
            job.setMapperClass(PageRankMapper.class);
        } else {
            job.setMapperClass(PageRankMapperInit.class);
        }
        job.setReducerClass(PageRankReducer.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(NodeWritable.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(NodeWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setNumReduceTasks(10);
        return job;
    }

    public int run(String[] args) throws Exception {
        String input  = args[0];
        String output = args[1];
        Configuration config = getConf();

        String inputFormat  = "%s/it%02d/part-r-*";
        String outputFormat = "%s/it%02d";

        iterationCount = config.getInt("iters", iterationCount);

        String inputStep, outputStep;
        ControlledJob[] steps = new ControlledJob[iterationCount];

        for (int i = 0; i < iterationCount; i++) {
            inputStep = (i > 0) ? String.format(inputFormat, output, i - 1) : input;
            outputStep = String.format(outputFormat, output, i);
            steps[i] = new ControlledJob(config);
            steps[i].setJob(getJobConf(inputStep, outputStep, i));
        }

        JobControl control = new JobControl(PageRankJob.class.getCanonicalName());
        for (ControlledJob step: steps) {
            control.addJob(step);
        }

        for (int i = 1; i < iterationCount; i++) {
            steps[i].addDependingJob(steps[i - 1]);
        }

        new Thread(control).start();
        while (!control.allFinished()) {
            System.out.println("Still running...");
            Thread.sleep(10000);
        }

        return control.getFailedJobList().isEmpty() ? 0 : 1;
    }

    static public void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new PageRankJob(), args);
        System.exit(ret);
    }
}
