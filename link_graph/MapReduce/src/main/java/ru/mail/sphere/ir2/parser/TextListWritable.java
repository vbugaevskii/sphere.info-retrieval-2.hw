package ru.mail.sphere.ir2.parser;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import java.util.LinkedList;
import java.util.List;

public class TextListWritable implements Writable {
    private List<Text> urlsList;

    public List<Text> getUrlsList() {
        return urlsList;
    }

    public int getUrlsListLength() {
        return urlsList.size();
    }

    public TextListWritable() {
        this.urlsList = new LinkedList<Text>();
    }

    public TextListWritable(List<String> urlsList) {
        this.urlsList = new LinkedList<Text>();
        for (String url: urlsList) {
            this.urlsList.add(new Text(url));
        }
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(urlsList.size());
        for (Text url : urlsList) {
            url.write(out);
        }
    }

    public void readFields(DataInput in) throws IOException {
        urlsList = new LinkedList<Text>();

        int urlsListSize = in.readInt();
        for (; urlsListSize > 0; urlsListSize--) {
            Text url = new Text();
            url.readFields(in);
            urlsList.add(url);
        }
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        for (Text url : urlsList) {
            stringBuilder.append(url.toString());
            stringBuilder.append(" ");
        }

        if (stringBuilder.length() > 0) {
            stringBuilder.delete(stringBuilder.length() - 1, stringBuilder.length());
        }

        return stringBuilder.toString();
    }
}
