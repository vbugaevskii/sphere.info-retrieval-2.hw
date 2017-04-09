package ru.mail.sphere.ir2.numerator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class Denumerator {
    private Map<Integer, String> urlsIndex = new HashMap<Integer, String>();

    public Denumerator(Path input) throws IOException {
        FileSystem fileSystem = FileSystem.get(new Configuration());
        FSDataInputStream indexInputStream = fileSystem.open(input);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(indexInputStream));

        urlsIndex.clear();
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

    public void map(Path input) throws IOException {
        FileSystem fileSystem = FileSystem.get(new Configuration());
        Path output = input.suffix("_denumerated");

        FSDataInputStream indexInputStream = fileSystem.open(input);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(indexInputStream));

        FSDataOutputStream indexOutputStream = fileSystem.create(output);
        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(indexOutputStream));

        String record, resultFormat = "http://lenta.ru%s\n";

        while ((record = bufferedReader.readLine()) != null) {
            String[] pair = record.split("\t");
            if (pair.length < 2) {
                continue;
            }

            Integer index = Integer.valueOf(pair[0]);
            if (urlsIndex.containsKey(index)) {
                bufferedWriter.write(String.format(resultFormat, urlsIndex.get(index)));
            }
        }

        bufferedWriter.close();
        indexOutputStream.close();

        bufferedReader.close();
        indexInputStream.close();
    }
}
