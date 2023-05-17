package org.spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class PageRank {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("PageRank Application");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> textFile = sc.textFile("hdfs://10.102.0.198:9000/ex3/input/DataSet");

        JavaPairRDD<String, List<String>> links = textFile.mapToPair(x -> {
            String[] a = x.split("\\s+", 2);
            String[] b = a[1].split(",");
            return new Tuple2<>(a[0], Arrays.asList(b));
        });

        JavaPairRDD<String, Double> rp = links.mapValues(x -> 1.0);

        for (int i = 0; i < 10; i++) {
            JavaPairRDD<String, Tuple2<List<String>, Double>> tmp = links.join(rp);
            JavaPairRDD<String, Double> contributions = tmp.flatMapToPair(x -> {
                List<String> ls = x._2._1;
                Double rank = x._2._2;

                List<Tuple2<String, Double>> ret = new ArrayList<>();
                for (String dest : ls) {
                    ret.add(new Tuple2<>(dest, rank/ls.size()));
                }
                return ret.iterator();
            });

            rp = contributions.reduceByKey(Double::sum).mapValues(v -> 0.15 + 0.85 * v);
        }
        rp.mapToPair(tp -> new Tuple2<>(tp._2,tp._1))
                .sortByKey(false)
                .mapToPair(tp -> new Tuple2<>(tp._2,tp._1))
                .saveAsTextFile("hdfs://10.102.0.198:9000/user/bigdata_202022300317/exp3/out");

        sc.close();
    }
}

