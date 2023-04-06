package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.mapreduce.InvertedIndex;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

public class StopWordsExample {

    static Set<String> stopWords = new HashSet<>();

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
        conf.set("fs.defaultFS", "hdfs://10.102.0.198:9000");
        FileSystem fs = FileSystem.get(new URI("hdfs://10.102.0.198:9000"), conf);
        FSDataInputStream inputStream = fs.open(new Path("/stop_words/stop_words_eng.txt"));
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        String str;
        while ((str = reader.readLine()) != null) {
            stopWords.add(str);
//            System.out.println(str);
        }
        System.out.println(stopWords.contains("a"));
    }
}
