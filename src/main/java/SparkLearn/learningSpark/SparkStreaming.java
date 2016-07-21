package SparkLearn.learningSpark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class SparkStreaming {
	public static void main(String[] args) {
		
		SparkConf sc = new SparkConf().setMaster("local[2]").setAppName("SparkStreaming");
		JavaStreamingContext jssc = new JavaStreamingContext(sc, new Duration(10000));
		JavaDStream<String> lines = jssc.textFileStream("data/dir/this");
//		JavaDStream<String> lines = jssc.socketTextStream("localhost", 7777);
//		JavaDStream<String> lines = jssc.socketTextStream("10.3.3.51", 7777);
//		JavaDStream<String> lines = jssc.socketTextStream("10.3.3.99", 7777);
//		JavaDStream<String> errorLines = lines.filter(line -> line.contains("GET"));
//		JavaDStream<String> errorLines = lines.filter(line -> line.contains("error"));
//		JavaDStream<String> errorLines = lines.filter(new Function<String, Boolean>() {
//			private static final long serialVersionUID = 1L;
//			@Override
//			public Boolean call(String line) throws Exception {
////				System.out.println(line);
////				return true;
//				return line.contains("0.4");
//			}
//		});
//		lines.count();
		lines.print();
//		errorLines.count();
//		errorLines.print();
//		
		jssc.start();
		jssc.awaitTermination();
	}
}
