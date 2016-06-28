package SparkLearn.learningSpark;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * 
 * @ClassName: FirstPairFunction
 * @Description: 4.2 4.3
 * @author: 蒋佩釗
 * @date: 2016年6月22日 上午11:24:55
 *
 */
public class FirstPairFunction {
	public static void main(String[] args) {
		SparkConf sc = new SparkConf().setAppName("FirstPairFunction");
		JavaSparkContext jsc = new JavaSparkContext(sc);
		JavaRDD<String> lines = jsc.textFile("D:\\behavior.txt", 1);
		System.out.println(lines.toDebugString());
		
		JavaPairRDD<String, String> pairs = lines.mapToPair(x -> new Tuple2<String, String>(x.split("data")[0], x));
		System.out.println(StringUtils.join(pairs.collect(), "\n"));
		
		JavaPairRDD<String, String> filter = pairs.filter(x -> x._2.contains("VIDE1327053569000253"));
		System.out.println(filter.toDebugString());
		System.out.println(StringUtils.join(filter.collect(), "\n"));
		System.out.println(lines.getNumPartitions());
		try {
			Thread.sleep(1000000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		jsc.close();
	}
}
