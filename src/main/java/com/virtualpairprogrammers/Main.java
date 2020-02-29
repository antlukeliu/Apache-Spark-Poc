package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

public class Main {
    public static void main(String [] args) {
        List<Double> inputData = new ArrayList<>();
        inputData.add(25.5);
        inputData.add(90.32);
        inputData.add(12.499999);
        inputData.add(28.28);

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        //Local cluster - * use available core - without [*] it will use a single thread
        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //Wrapper JavaRDD is implemented in Scala
        JavaRDD<Double> myRdd = sc.parallelize(inputData);

        Double result = myRdd.reduce( (value1, value2) -> value1 + value2 );

        System.out.println(result);

        sc.close();
    }
}
