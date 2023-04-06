package org.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class InvertedIndex {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
        conf.set("fs.defaultFS", "hdfs://10.102.0.198:9000");

        //获取运行时输入的参数，一般是通过shell脚本文件传进来。
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        String inputDir;
        String outputDir;
        if (otherArgs.length < 2) {
            System.err.println("No enough arguments");
            inputDir = "/input";
            outputDir = "/user/bigdata_202022300317/output";
//            System.exit(2);
        } else {
            inputDir = otherArgs[0];
            outputDir = otherArgs[1];
        }

        // get stop words
//        System.setProperty("HADOOP_USER_NAME", "bigdata_202022300317");
        FileSystem fs = FileSystem.get(new URI("hdfs://10.102.0.198:9000"), conf);
//        FSDataInputStream inputStream = fs.open(new Path("/stop_words/stop_words_eng.txt"));
//        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
//        String str;
//        while ((str = reader.readLine()) != null) {
//            Map.stopWords.add(str.trim());
//        }

        Job job = Job.getInstance();
        job.setJarByClass(InvertedIndex.class);
        job.setJobName("Inverted Index");
//        System.err.println("++++++++++++++++++++++++++++++++++++++");

        //设置读取文件的路径，都是从HDFS中读取。读取文件路径从脚本文件中传进来
        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(new Path(inputDir), true);
        while (iterator.hasNext()) {
            LocatedFileStatus fileStatus = iterator.next();
            Path path = fileStatus.getPath();
            FileInputFormat.addInputPath(job, path);
        }
        FileInputFormat.addInputPath(job, new Path("/stop_words/stop_words_eng.txt"));
        //设置mapreduce程序的输出路径，MapReduce的结果都是输入到文件中
        FileOutputFormat.setOutputPath(job, new Path(outputDir));

        //设置实现了map函数的类
        job.setMapperClass(Map.class);
        //设置实现了reduce函数的类
        job.setReducerClass(Reduce.class);

        //设置reduce函数的key值
        job.setOutputKeyClass(Text.class);
        //设置reduce函数的value值
        job.setOutputValueClass(Text.class);
//        System.err.println("****************************************");
//        Logger logger = LoggerFactory.getLogger(InvertedIndex.class);
//        logger.
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


    static class Map extends Mapper<LongWritable, Text, Text, Text> {
        private Set<String> stopWords;
        @Override
        public void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();

            stopWords = new HashSet<>();

            FileSystem fs = null;
            try {
                fs = FileSystem.get(new URI("hdfs://10.102.0.198:9000"), conf);
            } catch (URISyntaxException e) {
                throw new RuntimeException(e);
            }
            FSDataInputStream inputStream = fs.open(new Path("/stop_words/stop_words_eng.txt"));
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            String str;
            while ((str = reader.readLine()) != null) {
                stopWords.add(str.trim());
            }
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split("[^a-zA-Z0-9]");
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            for (String word : line) {
                word = word.trim().toLowerCase();
                if (word.length() > 0 && !stopWords.contains(word)) {
                    context.write(new Text(word), new Text(fileName));
//                    System.out.printf("%s <%s,%s>\n", word, fileName, key);
                }
            }
        }
    }

    static class Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashMap<String, Integer> documentCount = new HashMap<>();
            StringBuilder stringBuilder = new StringBuilder();
            for (Text i : values) {
                String str = i.toString();
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
//            System.out.printf("%s: %s\n", key, outputString);
        }
    }
}
