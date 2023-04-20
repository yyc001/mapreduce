package org.mapreduce.pagerank;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PageRankReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    private int N;

    @Override
    protected void setup(Context context) {
        N = context.getConfiguration().getInt("totalPageNum", 1);
    }

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context ) throws IOException, InterruptedException {
        // PR(i) <= (1-d)/n + d* \sum_{j->i} { PR(j)/L(j) }

        double pr = 0;
        for (DoubleWritable doubleWritable : values) {
            double ad = doubleWritable.get();
            pr += ad;
        }
        double alpha = 0.85;
        pr = pr * alpha + (1 - alpha) / N;
        context.write(key, new DoubleWritable(pr));
    }
}
