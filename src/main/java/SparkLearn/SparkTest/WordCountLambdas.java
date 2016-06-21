package SparkLearn.SparkTest;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class WordCountLambdas {
	private static final Pattern SPACE = Pattern.compile("data");

	public static void main(String[] args) {

		System.out.println("1-----------------------------");
		SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		JavaRDD<String> lines = ctx.textFile("D:\\behavior.txt", 1);

		System.out.println("2-----------------------------");
		JavaPairRDD<String, Integer> counts = lines.flatMap(s -> Arrays.asList(SPACE.split(s)))
												   .mapToPair(s -> new Tuple2<String, Integer>(s, 1))
												   .reduceByKey((Integer i1, Integer i2) -> i1 + i2);

		System.out.println("5-----------------------------");
		List<Tuple2<String, Integer>> output = counts.collect();
		for (Tuple2<?, ?> tuple : output) {
			System.out.println(tuple._1() + ": " + tuple._2());
		}
		System.out.println("6-----------------------------\n" + output.size());
		ctx.stop();
		ctx.close();
		System.out.println("7-----------------------------");
	}

}
// -Dspark.master=local