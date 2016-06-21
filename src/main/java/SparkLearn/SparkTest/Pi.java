package SparkLearn.SparkTest;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

/**
 * Hello world!
 *
 */
public class Pi 
{
	private static Integer NUM_SAMPLES = 100000;
    public static void main( String[] args )
    {
    	System.out.println("1-----------------------------");
        SparkConf sparkConf = new SparkConf().setAppName("Pi");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
    	System.out.println("2-----------------------------");
    	List<Integer> l = new ArrayList<Integer>(NUM_SAMPLES);
    	for (int i = 0; i < NUM_SAMPLES; i++) {
    	  l.add(i);
    	}

    	System.out.println("3-----------------------------");
    	long count = sc.parallelize(l).filter(new Function<Integer, Boolean>() {
			private static final long serialVersionUID = 1L;

			public Boolean call(Integer i) {
	    	    double x = Math.random();
	    	    double y = Math.random();
	    	    return x*x + y*y < 1;
	    	  }
    	}).count();
    	System.out.println("4-----------------------------");
    	System.out.println("Pi is roughly " + 4.0 * count / NUM_SAMPLES);
    	System.out.println("5-----------------------------");
    	sc.close();
    }
}
//-Dspark.master=local
