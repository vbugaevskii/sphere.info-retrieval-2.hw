package ru.mail.sphere.ir2.parser;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ParserMapper extends Mapper<LongWritable, Text, Text, TextListWritable> {
    private Map<Integer, String> urlsIndex = new HashMap<Integer, String>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        FileSystem fileSystem = FileSystem.get(new Configuration());
        FileSplit split = (FileSplit) context.getInputSplit();
        Path parentPath = split.getPath().getParent();
        Path indexPath = parentPath.suffix("/urls.txt");

        FSDataInputStream indexInputStream = fileSystem.open(indexPath);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(indexInputStream));

        String record;
        while ((record = bufferedReader.readLine()) != null) {
            String[] pair = record.split("\t");
            if (pair.length < 2) {
                continue;
            }
            urlsIndex.put(Integer.valueOf(pair[0]), pair[1]);
        }

        bufferedReader.close();
        indexInputStream.close();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] pair = value.toString().split("\t");
        if (pair.length < 2) {
            return;
        }

        pair[0] = Parser.parseUrl(urlsIndex.get(Integer.valueOf(pair[0])));

        if (pair[0] != null && pair[1] != null) {
            Text urlFrom = new Text(pair[0]);
            List<String> urlsTo = Parser.decodeAndParse(pair[1]);
            context.write(urlFrom, new TextListWritable(urlsTo));
        }
    }
}
