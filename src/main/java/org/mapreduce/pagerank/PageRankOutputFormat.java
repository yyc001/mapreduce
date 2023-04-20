package org.mapreduce.pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 自定义MyFileOutputFormat继承FileOutputFormat，实现其中的getRecordWriter方法；
 * 该方法返回一个RecordWriter对象，需先创建此对象，实现其中的write、close方法；
 * 文件通过FileSystem在write方法中写出到hdfs自定义文件中
 */
public class PageRankOutputFormat<K, V> extends FileOutputFormat<K, V> {

    @Override
    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        Configuration conf = job.getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        final FSDataOutputStream math = fs.create(new Path(conf.get("weightOutput")));
        return new RecordWriter<K, V>() {
            @Override
            public void write(K key, V value) throws IOException, InterruptedException {
                math.write(String.format("(%s,%s)\n", key.toString(), value.toString()).getBytes());
            }

            @Override
            public void close(TaskAttemptContext context) throws IOException, InterruptedException {
                if (math != null) {
                    math.close();
                }
            }
        };
    }

}
