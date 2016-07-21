package mllib;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class StreamingKmeansTraining {
	public static void main(String[] args) {
		SparkConf sc = new SparkConf().setMaster("local").setAppName("ReadAndWrite");
		System.setProperty("hadoop.home.dir", "D:/Tools/hadoop-2.6.4");
		JavaSparkContext jsc = new JavaSparkContext(sc);
		JavaRDD<String> lines = jsc.textFile("D:\\dir\\streaming_kmeans_data_test.txt", 1);
		lines.saveAsTextFile("D:\\dir\\training\\streaming_kmeans_data_test");
		jsc.close();
		System.out.println("End Training---------------------");
	}
}
