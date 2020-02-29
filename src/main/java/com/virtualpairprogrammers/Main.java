package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

//Resilient Distributed Dataset (RDD)
public class Main {
    public static void main(String [] args) {
        List<Integer> inputData = new ArrayList<>();
        inputData.add(25);
        inputData.add(90);
        inputData.add(12);
        inputData.add(28);

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        //Local cluster - * use available core - without [*] it will use a single thread
        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //Wrapper JavaRDD is implemented in Scala
        JavaRDD<Integer> originalIntegers = sc.parallelize(inputData);
        JavaRDD<Tuple2<Integer, Double>> sqrtRdd = originalIntegers.map(value -> new Tuple2<>(value, Math.sqrt(value)));
        
        sc.close();
    }
}
