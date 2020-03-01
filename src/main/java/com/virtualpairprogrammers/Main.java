package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

//Resilient Distributed Dataset (RDD)
public class Main {
    @SuppressWarnings("resource")
    public static void main(String [] args) {
        //Tuple
//        List<Integer> inputData = new ArrayList<>();
//        inputData.add(25);
//        inputData.add(90);
//        inputData.add(12);
//        inputData.add(28);
//
//        Logger.getLogger("org.apache").setLevel(Level.WARN);
//
//        //Local cluster - * use available core - without [*] it will use a single thread
//        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
//        JavaSparkContext sc = new JavaSparkContext(conf);
//
//        //Wrapper JavaRDD is implemented in Scala
//        JavaRDD<Integer> originalIntegers = sc.parallelize(inputData);
//        JavaRDD<Tuple2<Integer, Double>> sqrtRdd = originalIntegers.map(value -> new Tuple2<>(value, Math.sqrt(value)));

        List<String> inputData = new ArrayList<>();
        inputData.add("WARN: Tuesday 4 September 0405");
        inputData.add("ERROR: Tuesday 4 September 0408");
        inputData.add("FATAL: Wednesday 5 September 1632");
        inputData.add("ERROR: Friday 7 September 1854");
        inputData.add("WARN: Saturday 8 September 1942");
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> originalLogMessages = sc.parallelize(inputData);

        //Group for key can lead ot severe performance problems on realwork data
        //Use Reduce by key first
        JavaPairRDD<String, Long> pairRDD =  originalLogMessages.mapToPair(rawValue -> {
            String[] columns = rawValue.split(":");
            String level = columns[0];

            return new Tuple2<>(level, 1L);
        });
        JavaPairRDD<String, Long> sumRDD = pairRDD.reduceByKey( (value1, value2) -> value1 + value2);

        sumRDD.foreach( tuple -> System.out.println(tuple._1 + " has " + tuple._2 + " instances"));

        //Optimal code
//        sc.parallelize(inputData)
//                .mapToPair(rawValue -> new Tuple2<>(rawValue.split(":")[0] , 1L  ))
//                .reduceByKey((value1, value2) -> value1 + value2)
//                .foreach(tuple -> System.out.println(tuple._1 + " has " + tuple._2 + " instances"));
        sc.close();
    }
}
