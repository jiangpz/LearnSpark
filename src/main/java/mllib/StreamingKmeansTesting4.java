package mllib;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class StreamingKmeansTesting4 {
	public static void main(String[] args) {
		SparkConf sc = new SparkConf().setMaster("local").setAppName("ReadAndWrite");
		System.setProperty("hadoop.home.dir", "D:/Tools/hadoop-2.6.4");
		JavaSparkContext jsc = new JavaSparkContext(sc);
//		JavaRDD<String> lines = jsc.textFile("data/dir/streaming_kmeans_data_test.txt", 1);
//		lines.saveAsTextFile("data/dir/training/streaming_kmeans_data_test");
//		JavaRDD<String> lines1 = jsc.textFile("data/dir/streaming_kmeans_data_test0.txt", 1);
//		lines1.saveAsTextFile("data/dir/testing/streaming_kmeans_data_test0");
//		lines1 = jsc.textFile("data/dir/streaming_kmeans_data_test1.txt", 1);
//		lines1.saveAsTextFile("data/dir/testing/streaming_kmeans_data_test1");
//		lines1 = jsc.textFile("data/dir/streaming_kmeans_data_test2.txt", 1);
//		lines1.saveAsTextFile("data/dir/testing/streaming_kmeans_data_test2");
//		lines1 = jsc.textFile("data/dir/streaming_kmeans_data_test3.txt", 1);
//		lines1.saveAsTextFile("data/dir/testing/streaming_kmeans_data_test3");
//		lines1 = jsc.textFile("data/dir/streaming_kmeans_data_test4.txt", 1);
//		lines1.saveAsTextFile("data/dir/testing/streaming_kmeans_data_test4");
		JavaRDD<String> lines1 = jsc.textFile("data/dir/streaming_kmeans_data_test5.txt", 1);
		lines1.saveAsTextFile("data/dir/testing/streaming_kmeans_data_test5");
		jsc.close();
		System.out.println("End Testing---------------------");
	}
}
