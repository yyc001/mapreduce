package org.mapreduce;

import com.sun.xml.internal.rngom.ast.builder.GrammarSection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Scanner;

public class AdvancedInvertedIndex {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
        conf.set("fs.defaultFS", "hdfs://10.102.0.198:9000");

        //获取运行时输入的参数，一般是通过shell脚本文件传进来。
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("No enough arguments");
            System.exit(2);
        }
        String inputDir = otherArgs[0];
        String outputDir = otherArgs[1];

        FileSystem fs = FileSystem.get(new URI("hdfs://10.102.0.198:9000"), conf);

        Job job = Job.getInstance();
        job.setJarByClass(InvertedIndex.class);
        job.setJobName("Inverted Index");

        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(new Path(inputDir), true);
        while (iterator.hasNext()) {
            LocatedFileStatus fileStatus = iterator.next();
            Path path = fileStatus.getPath();
            FileInputFormat.addInputPath(job, path);
        }
        FileInputFormat.addInputPath(job, new Path("/stop_words/stop_words_eng.txt"));
        FileOutputFormat.setOutputPath(job, new Path(outputDir));
        job.setMapperClass(Map.class);
        job.setCombinerClass(Combine.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


    static class Map extends Mapper<LongWritable, Text, Text, Text> {
//        static Set<String> stopWords = new HashSet<>();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split("[^a-zA-Z0-9]");
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            for (String word : line) {
                word = word.trim().toLowerCase();
                if (word.length() > 0) {
                    context.write(new Text(word), new Text(fileName));
//                    System.out.printf("%s <%s,%s>\n", word, fileName, key);
                }
            }
        }
    }

    static class Combine extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashMap<String, Integer> documentCount = new HashMap<>();
            StringBuilder stringBuilder = new StringBuilder();
            for (Text i : values) {
                String str = i.toString();
                if (str.equals("stop_words_eng.txt")) {
                    return;
                }
                Integer cnt = documentCount.get(str);
                if(cnt == null) {
                    cnt = 1;
                } else {
                    cnt++;
                }
                documentCount.put(str, cnt);
            }
            int sum = 0;
            for(String str: documentCount.keySet()){
                stringBuilder.append(String.format("<%s,%d>;", str, documentCount.get(str)));
                sum += documentCount.get(str);
            }
            stringBuilder.append(String.format("<total,%d>.", sum));
            context.write(key, new Text(stringBuilder.toString()));
        }
    }

    static class Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder stringBuilder = new StringBuilder();
            int sum = 0;
            for(Text i: values){
                Scanner scan = new Scanner(i.toString());
                int num = scan.nextInt();
                String str = scan.next();
                sum += num;
                stringBuilder.append(String.format("<%s,%d>;", str, num));
            }
            stringBuilder.append(String.format("<total,%d>.", sum));
            context.write(key, new Text(stringBuilder.toString()));
        }
    }
}
