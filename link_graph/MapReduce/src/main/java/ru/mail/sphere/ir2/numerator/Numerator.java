package ru.mail.sphere.ir2.numerator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import java.util.HashMap;
import java.util.Map;

public class Numerator {
    private Map<String, Integer> urlsIndex = new HashMap<String, Integer>();

    public Numerator(Path input) throws IOException {
        FileSystem fileSystem = FileSystem.get(new Configuration());
        FSDataInputStream indexInputStream = fileSystem.open(input);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(indexInputStream));

        int urlsIndexSize = 0;
        String record;

        while ((record = bufferedReader.readLine()) != null) {
            String[] pair = record.split("\t");
            if (pair.length < 2) {
                continue;
            }

            if (!urlsIndex.containsKey(pair[0])) {
                urlsIndex.put(pair[0], urlsIndexSize++);
            }

            String[] urlsList = pair[1].split(" ");
            for (String url : urlsList) {
                if (!urlsIndex.containsKey(url)) {
                    urlsIndex.put(url, urlsIndexSize++);
                }
            }
        }

        bufferedReader.close();
        indexInputStream.close();
    }

    public void map(Path input) throws IOException {
        FileSystem fileSystem = FileSystem.get(new Configuration());
        Path output = input.suffix("_numerated");

        FSDataInputStream indexInputStream = fileSystem.open(input);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(indexInputStream));

        FSDataOutputStream indexOutputStream = fileSystem.create(output);
        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(indexOutputStream));

        String record;

        outer:
        while ((record = bufferedReader.readLine()) != null) {
            StringBuilder result = new StringBuilder();

            String[] pair = record.split("\t");
            if (pair.length < 2) {
                continue;
            }

            if (!urlsIndex.containsKey(pair[0])) {
                continue;
            } else {
                result.append(urlsIndex.get(pair[0]));
                result.append("\t");
            }

            String[] urlsList = pair[1].split(" ");
            for (String url : urlsList) {
                if (!urlsIndex.containsKey(url)) {
                    continue outer;
                } else {
                    result.append(urlsIndex.get(url));
                    result.append(" ");
                }
            }

            if (result.length() > 0) {
                result.delete(result.length() - 1, result.length());
            }
            result.append("\n");

            bufferedWriter.write(result.toString());
        }

        bufferedWriter.close();
        indexOutputStream.close();

        bufferedReader.close();
        indexInputStream.close();
    }

    public void dump(Path output) throws IOException {
        FileSystem fileSystem = FileSystem.get(new Configuration());
        FSDataOutputStream indexOutputStream = fileSystem.create(output);
        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(indexOutputStream));

        String outputFormat = "%d\t%s\n";
        for (Map.Entry<String, Integer> pair: urlsIndex.entrySet()) {
            bufferedWriter.write(String.format(outputFormat, pair.getValue(), pair.getKey()));
        }

        bufferedWriter.close();
        indexOutputStream.close();
    }

    public static void main(String[] args) throws Exception {
        Path inputPath = new Path(args[0]);
        Path outputPath = inputPath.getParent().suffix("/urls.idx");
        Numerator numerator = new Numerator(inputPath);
        numerator.map(inputPath);
        numerator.dump(outputPath);
    }
}
