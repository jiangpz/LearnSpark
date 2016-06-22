package SparkLearn.learningSpark;

import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * 
 * @ClassName: Square
 * @Description: 3.5的例子
 * @author: 蒋佩釗
 * @date: 2016年6月22日 上午10:17:08
 *		.\bin\spark-submit --master local --class SparkLearn.learningSpark.Square D:\1111.jar
 */
public class Square {
	public static void main(String[] args) {
		SparkConf sc = new SparkConf().setAppName("Square");
		JavaSparkContext jsc = new JavaSparkContext(sc);
		JavaRDD<Integer> rdd = jsc.parallelize(Arrays.asList(1,2,3,4));
		System.out.println("1-----------------");
		JavaRDD<Integer> result = rdd.map(x -> x*x);
		System.out.println(StringUtils.join(result.collect(), ","));
		System.out.println("2-----------------");
		JavaDoubleRDD doubleResult = rdd.mapToDouble(x -> x*x);
		System.out.println(StringUtils.join(doubleResult.collect(), ","));
		jsc.close();
	}
}
