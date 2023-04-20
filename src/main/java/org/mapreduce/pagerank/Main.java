package org.mapreduce.pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class Main {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://10.102.0.198:9000");

        String inputFile = "/ex3/input";
        String outputSeq = "/user/bigdata_202022300317/exp3/out";

        FSDataInputStream inputStream = FileSystem.get(conf).open(new Path(inputFile));
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        int count = 0;
        while (reader.readLine() != null) {
            count ++;
        }
        conf.setInt("totalPageNum", count);


        for (int i = 1; i <= 10; i++) {
            conf.setInt("iterNum", i);
            conf.set("weightCheckpoint", outputSeq + (i - 1) );
            conf.set("weightOutput", outputSeq + i);

            Job job = Job.getInstance(conf, "PageRank");
            job.setMapperClass(PageRankMapper.class);
            job.setReducerClass(PageRankReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(DoubleWritable.class);
            FileInputFormat.addInputPath(job, new Path(inputFile));
            job.setOutputFormatClass(PageRankOutputFormat.class);
            job.setNumReduceTasks(1);
            job.waitForCompletion(true);
        }
    }


}
