package ru.mail.sphere.ir2.parser;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ParserReducer extends Reducer<Text, TextListWritable, Text, TextListWritable> {
    @Override
    protected void reduce(Text key, Iterable<TextListWritable> values, Context context)
            throws IOException, InterruptedException {
        TextListWritable urlsTo = values.iterator().next();

        if (urlsTo.getUrlsListLength() > 0) {
            context.write(key, urlsTo);
        }
    }
}
