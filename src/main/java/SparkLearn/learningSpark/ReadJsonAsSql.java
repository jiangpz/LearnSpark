package SparkLearn.learningSpark;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

public class ReadJsonAsSql {
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "D:/Tools/hadoop-2.6.4");
		SparkConf sc = new SparkConf().setAppName("ReadJsonAsSql");
		JavaSparkContext jsc = new JavaSparkContext(sc);
		HiveContext hc = new HiveContext(jsc);
		DataFrame df = hc.read().json("userinfo.json");
		df.registerTempTable("df");
		DataFrame result = hc.sql("Select user.name, txt From df");
		System.out.println(StringUtils.join(result.collect(), "\n"));
	}
}
