package SparkLearn.SparkTest;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class WordCount {
	private static final Pattern SPACE = Pattern.compile("data");
	
	public static void main(String[] args) {

		System.out.println("1-----------------------------");
		SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		JavaRDD<String> lines = ctx.textFile("D:\\behavior.txt", 1);

		System.out.println("2-----------------------------");
		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			private static final long serialVersionUID = 1L;
			public Iterable<String> call(String s) {
				return Arrays.asList(SPACE.split(s));
			}
		});

		System.out.println("3-----------------------------");
		JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
			private static final long serialVersionUID = 1L;
			public Tuple2<String, Integer> call(String s) {
				return new Tuple2<String, Integer>(s, 1);
			}
		});

		System.out.println("4-----------------------------");
		JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		});

		System.out.println("5-----------------------------");
		List<Tuple2<String, Integer>> output = counts.collect();
		for (Tuple2<?, ?> tuple : output) {
			System.out.println(tuple._1() + ": " + tuple._2());
		}
		System.out.println("6-----------------------------");
		ctx.stop();
		ctx.close();
		System.out.println("7-----------------------------");
	}

}
//-Dspark.master=local