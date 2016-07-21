package mllib;

import org.apache.spark.SparkConf;
import org.apache.spark.mllib.clustering.StreamingKMeans;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.util.Utils;

import scala.Tuple2;

public class StreamingKmeansExample {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Power iteration clustering Example");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(8000));
		
		try{
			JavaDStream<Vector> trainingData = jssc.textFileStream("data/dir/training").map(x -> Vectors.parse(x));
			JavaDStream<LabeledPoint> testData = jssc.textFileStream("data/dir/testing").map(x -> LabeledPoint.parse(x));
			int numDimensions = 3;
			int numClusters = 2;
			StreamingKMeans model = new StreamingKMeans()
										.setK(numClusters)
										.setDecayFactor(1.0)
										.setRandomCenters(numDimensions, 0.0, Utils.random().nextLong());
			
			model.trainOn(trainingData);
			model.predictOnValues(testData.mapToPair(lp -> new Tuple2<Double, Vector>(lp.label(), lp.features()))).print();
			
			jssc.start();
			jssc.awaitTermination();
		} finally {
			jssc.close();
		}
	}
}
