package OpinionMining.Miner;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import edu.stanford.nlp.tagger.maxent.MaxentTagger;

/**
 * 
 * @author Prasad Marla
 *
 */
public class App {
	public static void main(String[] args) {
		
		//Initialize spark config
		SparkConf sparkConf = new SparkConf().setAppName("Sample App"); 
		sparkConf.setMaster("local[8]");
		
		JavaSparkContext context = new JavaSparkContext(sparkConf);
		
		JavaRDD<String> file = context.textFile("D:/Sem1/bigdata/rt-polarity.pos");
		
		JavaRDD<String> words = file.flatMap(new FlatMapFunction<String, String>() {
			public Iterable<String> call(String s) throws Exception {
				List<String> container = new ArrayList<String>();
				/*Sentence sentence = new Sentence(s);
				container.addAll(sentence.posTa);*/
				    // The tagged string
				MaxentTagger tagger = new MaxentTagger(
		                "edu/stanford/nlp/models/pos-tagger/english-left3words/english-left3words-distsim.tagger");
		    
			        String tagged = tagger.tagString(s);
			        container.add(tagged);
				return  container;
			}
		});
		
		JavaPairRDD<String, Integer> pairs =  words.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String s) { 
				return new Tuple2<String, Integer>(s, 1); }
		});
      
		JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {				
			public Integer call(Integer a, Integer b) throws Exception {
				// TODO Auto-generated method stub
				return a+b;
			}
		});
			
		counts.coalesce(1).saveAsTextFile("D:/MovieOutput"); 
		context.close();

	}
}
