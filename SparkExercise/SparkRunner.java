package assg2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * 
 * @author Prasad Marla
 *
 */

public class SparkRunner {
	public static void main(String[] args) {
		
		//Initialize spark config
		SparkConf sparkConf = new SparkConf().setAppName("Sample App"); 
		//sparkConf.setMaster("local[8]");
		
		//input path represents warc file path. 
		final String inputPath = args[0];
		
		//extension to be appended for each wet file
		final String extension = "s3://commoncrawl/";
		
		//Initialize spark context
		JavaSparkContext context = new JavaSparkContext(sparkConf);
		
		//code for aws key value setting
		//context.hadoopConfiguration().set("fs.s3n.awsAccessKeyId","AKIAI56OOJ3RXMV5WUCA");
		//context.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey","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");
		 
		//nfiles represent number of wet files to be considered.
		final int nfiles = Integer.parseInt(args[1]);
		
		//RDD for wet paths.
		JavaRDD<String> mainfile = context.textFile(inputPath);
		
		JavaRDD<String> filepath = mainfile.flatMap(new FlatMapFunction<String, String>() {
			public Iterator<String> call(String s) throws Exception {
				List<String> container = new ArrayList<String>();
				container.add(extension+s); 
				return container.iterator();
			}
		});
		
		//Regular expression for word count.
		final Pattern p = Pattern.compile("i feel ([a-z',.?!#]+)"); 
		JavaRDD<String> file = context.textFile(String.join(",",filepath.take(nfiles)));
		
		/**
		 * Transformations applied to split
		 * 1) Find matching expressions
		 * 2) For pairs of occurrence of word
		 * 3) Combine count of words.
		 * 4) Filter words with count greater then 30
		 *  Action applied
		 *  output all word counts in a single file on a single partition. 
		 */
		
		JavaRDD<String> words = file.flatMap(new FlatMapFunction<String, String>() {
			public Iterator<String> call(String s) throws Exception {
				Matcher m = p.matcher(s.toLowerCase());
				List<String> container = new ArrayList<String>();
				
				while(m.find()){
					container.add(m.group(1));
				}
				return container.iterator();
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
		
		JavaPairRDD<String, Integer> desiredcounts = counts.filter(new Function<Tuple2<String,Integer>,Boolean>() {							
			public Boolean call(Tuple2<String, Integer> val) throws Exception {
				// TODO Auto-generated method stub
				if(val._2 > 30){
					return true;
				}
				else{
					return false;
				}
			}
		});
			
		desiredcounts.coalesce(1).saveAsTextFile("SparkOutput1"); 
		context.close();

	}
}
