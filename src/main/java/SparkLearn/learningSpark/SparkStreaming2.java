package SparkLearn.learningSpark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class SparkStreaming2 {
	public static void main(String[] args) {
		SparkConf sc = new SparkConf().setAppName("SparkStreaming");
		JavaStreamingContext jssc = new JavaStreamingContext(sc, new Duration(10000));
//		JavaDStream<String> lines = jssc.socketTextStream("localhost", 7777);
		JavaDStream<String> lines = jssc.socketTextStream("10.3.3.51", 7777);
//		JavaDStream<String> lines = jssc.socketTextStream("10.3.3.99", 7777);
//		JavaDStream<String> errorLines = lines.filter(line -> line.contains("GET"));
//		JavaDStream<String> errorLines = lines.filter(line -> line.contains("error"));
		JavaDStream<String> errorLines = lines.filter(new Function<String, Boolean>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Boolean call(String line) throws Exception {
				System.out.println(line);
				return true;
			}
		});
		System.out.println("11111111111111");
		lines.print();
		errorLines.print();
		System.out.println("11111111111111");
		jssc.start();
		jssc.awaitTermination();
	}
}
