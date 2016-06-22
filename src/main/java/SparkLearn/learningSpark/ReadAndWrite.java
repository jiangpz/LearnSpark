package SparkLearn.learningSpark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class ReadAndWrite {
	public static void main(String[] args) {
		SparkConf sc = new SparkConf().setAppName("ReadAndWrite");
		System.setProperty("hadoop.home.dir", "D:/Tools/hadoop-2.6.4");
		JavaSparkContext jsc = new JavaSparkContext(sc);
		System.out.println("1---------------------");
		JavaRDD<String> lines = jsc.textFile("D:\\content", 1);
		System.out.println("2---------------------");
		lines.saveAsTextFile("behavior111");
		System.out.println("3---------------------");
		jsc.close();
	}
}
