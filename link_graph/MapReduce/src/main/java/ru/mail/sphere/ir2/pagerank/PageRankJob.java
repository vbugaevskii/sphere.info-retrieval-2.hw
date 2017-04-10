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
    static final String parameterN        = "pagerank.n";
    static final String parameterAlpha    = "pagerank.alpha";
    static final String parameterLeftRank = "pagerank.rank";
    static final String getParameterIters = "pagerank.iters";

    static final float DEFAULT_ALPHA = 0.75f;

    static final int DEFAULT_N = 1147103;
    static final int DEFAULT_ITERS = 30;

    private Job getJobConfStepFirst(String input, String output, int currentIteration) throws IOException {
        Job job = Job.getInstance(getConf());

        job.setJarByClass(PageRankJob.class);
        job.setJobName(String.format("PageRank. Iteration=%02d. Step 1", currentIteration));

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        if (currentIteration > 0) {
            job.setMapperClass(PageRankMapperStepFirst.class);
            job.setReducerClass(PageRankReducerStepFirst.class);
        } else {
            job.setMapperClass(PageRankMapperInit.class);
            job.setReducerClass(PageRankReducerInit.class);
        }

        job.setOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(NodeWritable.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(NodeWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setNumReduceTasks(1);
        return job;
    }

    private Job getJobConfStepSecond(String input, String output, int currentIteration) throws IOException {
        Job job = Job.getInstance(getConf());

        job.setJarByClass(PageRankJob.class);
        job.setJobName(String.format("PageRank. Iteration=%02d. Step 2", currentIteration));

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setMapperClass(PageRankMapperStepSecond.class);
        job.setReducerClass(PageRankReducerStepSecond.class);

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

        String inputFormat  = "%s/it%02d/step%02d/part-r-*";
        String outputFormat = "%s/it%02d/step%02d/";

        int iterationCount = config.getInt(getParameterIters, DEFAULT_ITERS);
        iterationCount *= 2;
        iterationCount += 1;

        String inputStep, outputStep;
        ControlledJob[] steps = new ControlledJob[iterationCount];

        inputStep  = input;
        outputStep = String.format(outputFormat, output, 0, 2);
        steps[0] = new ControlledJob(config);
        steps[0].setJob(getJobConfStepFirst(inputStep, outputStep, 0));

        for (int i = 1, step = 2, currentIteration = 1; i < iterationCount; i += 2) {
            inputStep  = String.format(inputFormat,  output, currentIteration - 1, step--);
            outputStep = String.format(outputFormat, output, currentIteration, step);
            steps[i] = new ControlledJob(config);
            steps[i].setJob(getJobConfStepFirst(inputStep, outputStep, currentIteration));

            inputStep  = String.format(inputFormat,  output, currentIteration, step++);
            outputStep = String.format(outputFormat, output, currentIteration, step);
            steps[i + 1] = new ControlledJob(config);
            steps[i + 1].setJob(getJobConfStepSecond(inputStep, outputStep, currentIteration));
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
