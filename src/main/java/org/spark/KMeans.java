package org.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.*;
import java.util.*;

import scala.Tuple2;

public class KMeans {
    static int ITER = 10;
    public static void main(String[] args) throws IOException {
        KMeans self = new KMeans();
        String path = "iris_dataset.txt";
        List<Vector<Double>> data = new ArrayList<>();
        BufferedReader br = new BufferedReader(new FileReader(path));
        String line;
        Map<Integer, List<Vector<Double>>> calc_ct = new HashMap<>();
        while ((line = br.readLine()) != null) {
            Vector<Double> p = new Vector<>();
            String[] k = line.split(" ");
            for(int i=0; i<k.length-1; i++) {
                p.add(Double.parseDouble(k[i]));
            }
            data.add(p);
            int key = Integer.parseInt(k[k.length-1]);
            List<Vector<Double>> l;
            if(calc_ct.containsKey(key)) {
                l = calc_ct.get(key);
            } else {
                l = new ArrayList<>();
            }
            l.add(p);
            calc_ct.put(key, l);
        }
        List<Vector<Double>> real_ct = new ArrayList<>();
        for(Map.Entry<Integer, List<Vector<Double>>> k: calc_ct.entrySet()){
            List<Vector<Double>> v = k.getValue();
            Vector<Double> center = new Vector<>(Collections.nCopies(v.get(0).size(), 0.0));
            for(Vector<Double> a: v) {
                center = vecadd(center, a);
            }
            center = vecdiv(center, (double)v.size());
            real_ct.add(center);
        }
        List<Vector<Double>> init_center = new ArrayList<>();
        Random random = new Random();
        for(int i=0; i<real_ct.size(); i++){
            Vector<Double> vector = new Vector<>();
            for (int j = 0; j < real_ct.get(0).size(); j++) {
                vector.add(real_ct.get(i).get(j) + random.nextDouble()-0.5);
            }
            init_center.add(vector);
        }
        FileWriter writer = new FileWriter("output.txt");

        writer.write("Real center=\n");
        for(Vector<Double> v: real_ct){
            for(Double d: v) {
                writer.write(String.format("%f ", d));
            }
            writer.write("\n");
        }

        writer.write("Initial center=\n");
        for(Vector<Double> v: init_center){
            for(Double d: v) {
                writer.write(String.format("%f ", d));
            }
            writer.write("\n");
        }

        List<Vector<Double>> direct = self.directSolution(data, init_center);
        writer.write("Direct center=\n");
        for(Vector<Double> v: direct){
            for(Double d: v) {
                writer.write(String.format("%f ", d));
            }
            writer.write("\n");
        }

        List<Vector<Double>> sparkSolution = self.sparkSolution(data, init_center);
        writer.write("Spark center=\n");
        for(Vector<Double> v: sparkSolution){
            for(Double d: v) {
                writer.write(String.format("%f ", d));
            }
            writer.write("\n");
        }
        writer.close();
    }

    private List<Vector<Double>> directSolution(List<Vector<Double>> data, List<Vector<Double>> init_center) {
        List<Vector<Double>> now_center = new ArrayList<>(init_center);
        int[] now_class = new int[data.size()];
        int K = now_center.size();
        for(int iter=0; iter<ITER; iter++) {
            for(int i=0; i<data.size(); i++) {
                Double n_dis = 1e9+7;
                for(int l=0; l<K; l++) {
                    Vector<Double> center = now_center.get(l);
                    Double distance = calc_distance(center, data.get(i));
                    if(distance < n_dis) {
                        now_class[i] = l;
                        n_dis = distance;
                    }
                }
            }
            for(int l=0; l<K; l++) {
                Vector<Double> center = new Vector<>(Collections.nCopies(now_center.get(l).size(), 0.0));
                int num = 0;
                for (int i = 0; i < data.size(); i++) {
                    if (now_class[i] == l) {
                        center = vecadd(center, data.get(i));
                        num++;
                    }
                }
                now_center.set(l, vecdiv(center, (double) num));
            }

        }

        return now_center;
    }

    private static Vector<Double> vecadd(Vector<Double> a, Vector<Double> b) {
        Vector<Double> v = new Vector<>();
        for(int i=0; i<a.size(); i++) {
            v.add(a.get(i) + b.get(i));
        }
        return v;
    }

    private static Vector<Double> vecdiv(Vector<Double> center, Double num) {
        Vector<Double> v = new Vector<>();
        for(Double d: center) {
            v.add(d / num);
        }
        return v;
    }

    private static Double calc_distance(Vector<Double> a, Vector<Double> b) {
        double ret = 0;
        for(int i=0; i<a.size(); i++) {
            double d = a.get(i) - b.get(i);
            ret += d*d;
        }
        return ret;
    }

    private List<Vector<Double>> sparkSolution(List<Vector<Double>> data, List<Vector<Double>> init_center) {
        SparkConf conf = new SparkConf().setAppName("KMeans Application");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Vector<Double>> p = sc.parallelize(data);
        List<Vector<Double>> now_center = new ArrayList<>(init_center);


        for(int iter=0; iter<ITER; iter++) {
            List<List<Vector<Double>>> lNow_center = new ArrayList<>();
            lNow_center.add(now_center);
            JavaRDD<List<Vector<Double>>> finalNow_center = sc.parallelize(lNow_center);
            now_center = p.cartesian(finalNow_center)
                    .mapToPair(x-> {
                        double now_dis = 1e9+7;
                        int cls = -1;
                        for(int i=0; i<x._2.size(); i++) {
                            double distance = calc_distance(x._1, x._2.get(i));
                            if(now_dis > distance) {
                                now_dis = distance;
                                cls = i;
                            }
                        }
                        return new Tuple2<>(x._1, cls);
                    })
                    .mapToPair(tp -> new Tuple2<>(tp._2, new Tuple2<>(tp._1, 1)))
                    .reduceByKey((a, b) -> new Tuple2<>(vecadd(a._1, b._1), a._2+b._2))
                    .map(pair -> vecdiv(pair._2._1, (double)pair._2._2))
                    .collect();
        }

        return now_center;
    }
}
