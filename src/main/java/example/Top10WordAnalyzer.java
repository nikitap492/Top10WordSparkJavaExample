package example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public final class Top10WordAnalyzer {

    public static void main(String[] args)  {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("Top10"));

        sc.textFile(args[0]).map(line -> line.replaceAll("[^A-Za-z]+", " "))
                .map(String::toLowerCase)
                .flatMap(line -> Arrays.asList(line.split("\\s+")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1L))
                .reduceByKey((x, y) ->  x + y)
                .collect()
                .stream()
                .sorted((t1, t2) -> t2._2.compareTo(t1._2))
                .map(tuple -> tuple._1() + ": " + tuple._2())
                .limit(10)
                .forEach(System.out::println);

        sc.stop();
    }
}