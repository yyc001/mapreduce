package org.mapreduce.grandchild;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class Main {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //获取运行时输入的参数，一般是通过shell脚本文件传进来。
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Not enough arguments");
            System.exit(2);
        }

        Job job = Job.getInstance();
        job.setJarByClass(Main.class);
        job.setJobName("Grandchild detection");

        //设置读取文件的路径，都是从HDFS中读取。读取文件路径从脚本文件中传进来
        for(int i = 0; i <otherArgs.length - 1;i++){
            FileInputFormat.addInputPath(job,new Path(otherArgs[i]));
        }
        //设置mapreduce程序的输出路径，MapReduce的结果都是输入到文件中
        FileOutputFormat.setOutputPath(job,new Path(otherArgs[otherArgs.length -1]));

        //设置实现了map函数的类
        job.setMapperClass(Map.class);
        //设置实现了reduce函数的类
        job.setReducerClass(Reduce.class);

        //设置reduce函数的key值
        job.setOutputKeyClass(Text.class);
        //设置reduce函数的value值
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


    static class Map extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split(" ");
            String child = line[0];
            String parent = line[1];
            context.write(new Text(parent), new Text("c " + child));
            context.write(new Text(child), new Text("p " + parent));
        }
    }

    static class Reduce extends Reducer<Text, Text, Text, Text>{
        public void reduce(Text key, Iterable<Text> values,Context context) throws IOException,InterruptedException{
            Set<String> children = new HashSet<>();
            Set<String> parents = new HashSet<>();
            for(Text val: values){
                String[] line = val.toString().split(" ");
                String opt = line[0];
                String name = line[1];
                if(opt.equals("c")) {
                    children.add(name);
                } else {
                    parents.add(name);
                }
            }
            for(String child: children) {
                for(String parent: parents) {
                    context.write(new Text(child), new Text(parent));
                }
            }
        }
    }
}
