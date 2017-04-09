package ru.mail.sphere.ir2.parser;

import org.apache.commons.codec.binary.Base64InputStream;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.*;

import java.net.MalformedURLException;
import java.net.URL;

import java.util.*;
import java.util.zip.InflaterInputStream;

public final class Parser {
    public static String decode(String data) throws IOException {
        InputStream inputStream = new InflaterInputStream(new Base64InputStream(
                new ByteArrayInputStream(data.getBytes())));

        BufferedReader bufferedReader = new BufferedReader(
                new InputStreamReader(inputStream));

        StringBuilder result = new StringBuilder();

        try {
            String record;
            while ((record = bufferedReader.readLine()) != null) {
                result.append(record);
                result.append("\n");
            }
        } catch (IOException err) {
            return null;
        } finally {
            bufferedReader.close();
            inputStream.close();
        }

        return (result.length() > 0) ? result.toString(): null;
    }

    public static String parseUrl(String href) {
        if (href == null) {
            return null;
        }

        String result = null;

        try {
            URL url = new URL(href);

            if (url.getHost() == null || url.getHost().equals("lenta.ru")) {
                result = url.getPath();
                if (result == null || result.length() == 0) {
                    result = "/";
                }
            } else {
                result = null;
            }
        } catch (MalformedURLException err) {
            if (href.length() == 0) {
                result = "/";
            } else if (href.charAt(0) == '/') {
                result = href;
            }
        }

        return result;
    }

    public static List<String> parse(String data) throws IOException {
        Document document = Jsoup.parse(data);
        Elements elements = document.select("a[href]");

        Set<String> links = new HashSet<String>();
        for (Element e: elements) {
            String href = Parser.parseUrl(e.attr("href"));
            if (href != null) {
                links.add(href);
            }
        }

        return new ArrayList<String>(links);
    }

    public static List<String> decodeAndParse(String compressedData) throws IOException {
        String decodedData = decode(compressedData);
        return (decodedData != null) ? parse(decodedData) : new LinkedList<String>();
    }
}
