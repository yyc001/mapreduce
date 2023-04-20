package org.mapreduce.pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

public class PageRankMapper extends Mapper<Object, Text, Text, DoubleWritable> {

    private Map<String, Double> weight;
    private double initialWeight = 1;

    @Override
    protected void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        int iterNum = conf.getInt("iterNum", 1);
        if (iterNum == 1) {
            initialWeight = 1.0 / conf.getInt("totalPageNum", 1);
        } else {
            weight = new HashMap<>();
            FSDataInputStream inputStream = FileSystem.get(conf).open(new Path(conf.get("weightCheckpoint")));
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            String str;
            while ((str = reader.readLine()) != null) {
                String[] mp = str.trim().replaceAll("[()]", "").split(",");
                weight.put(mp[0], Double.parseDouble(mp[1]));
            }
        }
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // PR(i) <= (1-d)/n + d* \sum_{j->i} { PR(j)/L(j) }

        String[] line = value.toString().split("\t");
        String node = line[0];
        Configuration conf = context.getConfiguration();
        double p;
        if (conf.getInt("iterNum", 1) == 1) {
            p = initialWeight;
        } else {
            p = weight.get(node);
        }
        StringTokenizer stringTokenizer = new StringTokenizer(line[1], ",");
        int l = stringTokenizer.countTokens();
        p /= l;
        while (stringTokenizer.hasMoreTokens()) {
            String nxt = stringTokenizer.nextToken();
            context.write(new Text(nxt), new DoubleWritable(p));
        }
    }
}