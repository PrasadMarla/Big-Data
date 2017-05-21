package CollabrativeFiltering.ItemitemFiltering;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;


/**
 * 
 * @author Prasad Marla
 *
 */

/**
 *Container Class for holding each row of utility matrix. 
 */
class UserVectorContainer implements Serializable,Comparable<UserVectorContainer>{
	HashMap<String,Double> userRatings;
	double avgSum;
	double sim;
	@Override
	public String toString() {
		StringBuffer stringBuilder = new StringBuffer();
		for (String string : userRatings.keySet()) {
			stringBuilder.append("UserId:"+string+" Rating:"+userRatings.get(string)+"\n");
		}
		stringBuilder.append("Avg:"+avgSum+ " Sim: "+sim+"\n");
		return stringBuilder.toString();
	}
	public int compareTo(UserVectorContainer vec) {
		// TODO Auto-generated method stub
		if(this.sim-vec.sim > 0.0){
			return 1;
		}
		else if(this.sim-vec.sim == 0.0){
			return 0;
		}
		else{
			return -1;
		}
	}
}

public class App 
{
	//private static JavaSparkContext context;
	private static SparkSession spark;

	private static void init() {

		spark = SparkSession.builder()/*master("local[8]")*/.appName("Recommendtation").getOrCreate();
		spark.conf().set("spark.sql.shuffle.partitions",""+100);
	}
	private static Dataset<Row> readDataSet(String inputPath) {

		//String inputPath = "D:/Sem1/bigdata/Assg3/ratings_Video_Games10K.csv";
		RDD<String> stringTuples = spark.sparkContext().textFile(inputPath,0);

		StructType xactSchema = new StructType(
				new StructField[] { new StructField("user", DataTypes.StringType, true, Metadata.empty()),
						new StructField("item", DataTypes.StringType, true, Metadata.empty()),
						new StructField("ratings", DataTypes.DoubleType, true, Metadata.empty()),
						new StructField("timestamp", DataTypes.StringType, true, Metadata.empty()) });

		Dataset<Row> datasetCSV = spark.read().format("org.apache.spark.csv").schema(xactSchema)
				.option("header", "false").csv(inputPath);

		// create DataFrame from xactRDD, with the specified schema
		return datasetCSV;
	}

	public static void main(String args[]) throws IOException {

		// code for aws key value setting
		// context.hadoopConfiguration().set("fs.s3n.awsAccessKeyId","AKIAI56OOJ3RXMV5WUCA");
		// context.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey","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");
		/*********************************DataSet Creation Begin***************************/ 
		init();
		Properties props = new Properties();
		props.put("spark.sql.crossJoin.enabled", "true");
		spark.conf().set("spark.sql.crossJoin.enabled", true);
		String inputPath = args[0];//"D:/Sem1/bigdata/Assg3/ratings_Video_Games.csv";
		final String calItemid = args[1]; //"B000035Y4P";
		String outputPath = args[2];
		Dataset<Row> DatasetCSV = readDataSet(inputPath);
		//filteredItems.javaRDD().coalesce(1).saveAsTextFile("D:/temp9");
		DatasetCSV.createOrReplaceTempView("Dataset");
		Dataset<Row> filteredItems = spark.sql(
				"select item, user,ratings from Dataset where item in (SELECT DISTINCT item FROM Dataset group by item having count(item) >= 25)");


		// filteredItems.printSchema();
		filteredItems.createOrReplaceTempView("filteredStage1");

		Dataset<Row> usersWithMoreThen10Buys = spark
				.sql("SELECT DISTINCT user FROM filteredStage1 GROUP BY user HAVING count(user) >= 10");
		usersWithMoreThen10Buys.persist();
		long basketCount = usersWithMoreThen10Buys.count();

		usersWithMoreThen10Buys.createOrReplaceTempView("GroupedUser");
		Dataset<Row> filteredItemStage2 = spark.sql(
				"select filteredStage1.item, filteredStage1.user,filteredStage1.ratings from filteredStage1, GroupedUser where filteredStage1.user = GroupedUser.user ");

		filteredItemStage2.persist(StorageLevel.MEMORY_AND_DISK_SER());

		JavaPairRDD<String,UserVectorContainer> vectorForm = filteredItemStage2.javaRDD().mapToPair(new PairFunction<Row, String, UserVectorContainer>() {

			public Tuple2<String, UserVectorContainer> call(Row row) throws Exception {
				// TODO Auto-generated method stub
				UserVectorContainer listvectorContainer = new UserVectorContainer();
				HashMap<String,Double> userIDRatings = new HashMap<String, Double>();
				userIDRatings.put(row.getString(1),row.getDouble(2));
				listvectorContainer.avgSum = row.getDouble(2);
				listvectorContainer.userRatings = userIDRatings;

				return new Tuple2<String, UserVectorContainer>(row.getString(0),listvectorContainer);
			}
		});

		JavaPairRDD<String, UserVectorContainer> result = vectorForm.reduceByKey(new Function2<UserVectorContainer, UserVectorContainer, UserVectorContainer>() {

			public UserVectorContainer call(UserVectorContainer list1,
					UserVectorContainer list2) throws Exception {
				list1.userRatings.putAll(list2.userRatings);
				list1.avgSum = (list1.avgSum + list2.avgSum);
				return list1;
			}
		});

		List<UserVectorContainer> targetItem = result.lookup(calItemid);
		targetItem.get(0).avgSum = targetItem.get(0).avgSum / targetItem.get(0).userRatings.size();
		//targetItem.get(0).sim = -999;
		final UserVectorContainer target = targetItem.get(0);

		Set<String> userVar = target.userRatings.keySet();
      
		/*********************************DataSet Creation Ends***************************/
         /**
          * similarity calculation where target item and rest of the items similarity is created 
          */
  		JavaRDD<Tuple2<String, UserVectorContainer>> result1 = result.map(new Function<Tuple2<String,UserVectorContainer>,Tuple2<String,UserVectorContainer>>() {

			public Tuple2<String, UserVectorContainer> call(
					Tuple2<String, UserVectorContainer> tuple)
							throws Exception {

				if(tuple._1 == calItemid){
					return new Tuple2<String, UserVectorContainer>(calItemid, target);
				}
				HashMap<String, Double> targetMap = target.userRatings;
				double targetAvg = target.avgSum;
				HashMap<String, Double> similarMap = tuple._2.userRatings;
				double similarItemAvg = tuple._2.avgSum/similarMap.size();
				double targetsqrSum = 0.0;//target.squareSum;
				double similarItemsqrSum =0.0; //tuple._2.squareSum; 
				double dotproduct = 0.0;

				for(String key :similarMap.keySet()){
					double similarItemVal = similarMap.get(key) - similarItemAvg;
					similarItemsqrSum += Math.pow(similarItemVal,2);
				}

				for(String key :targetMap.keySet()){
					double targetVal = targetMap.get(key)- targetAvg;
					//System.out.println(targetVal);
					targetsqrSum += Math.pow(targetVal,2);
					//System.out.println(targetsqrSum); 
					if(similarMap.containsKey(key)){
						double similarItemVal = similarMap.get(key) - similarItemAvg;
						dotproduct = dotproduct + (targetVal*similarItemVal);
					}	 
				}

				double sim =0.0;
				if(dotproduct != 0.0){
					sim = dotproduct/(Math.sqrt(targetsqrSum)*Math.sqrt(similarItemsqrSum));		
				}

				//tuple._2.avgSum = similarItemAvg;
				UserVectorContainer userVectorContainer = new UserVectorContainer();
				userVectorContainer.avgSum = similarItemAvg;
				userVectorContainer.userRatings = tuple._2.userRatings;
				userVectorContainer.sim = sim;
				return new Tuple2<String, UserVectorContainer>(tuple._1, userVectorContainer);
			}
		}).persist(StorageLevel.MEMORY_AND_DISK());

		//result1.coalesce(1).saveAsTextFile("D:/OutputSim3");


		JavaRDD<String> userIDs = usersWithMoreThen10Buys.javaRDD().map(new Function<Row, String>() {
			public String call(Row row) throws Exception {
				return row.getString(0);
			}
		});

		List<String> userList = userIDs.collect();
		List<String> calculateUserList = new ArrayList<String>();

		/**
		 * Calculate userids for which rating has to be calculated.
		 */
		for (String string : userList) {
			if(!userVar.contains(string)){
				calculateUserList.add(string);
			}
		}

		List<String> RatingList = new ArrayList<String>();
		int itr =calculateUserList.size();
		
		/**
		 * Calculate Actual User ratings.
		 */

		for(int i =0;i < itr;i++){
			/*if(i==1000){
				break;
		    }*/
			final String userId = calculateUserList.get(i);

			System.out.println(calculateUserList.size());

			JavaRDD<Tuple2<String, UserVectorContainer>> sampleUserSet = result1.filter(new Function<Tuple2<String,UserVectorContainer>, Boolean>() {
				public Boolean call(Tuple2<String, UserVectorContainer> tuple)
						throws Exception {
					return tuple._2.userRatings.containsKey(userId);	
				} 	
			});

			List<Tuple2<String, UserVectorContainer>> calculateUserSet = sampleUserSet.collect();
			sampleUserSet.unpersist();
			List<UserVectorContainer> userVectorList = new ArrayList<UserVectorContainer>();

			for (Tuple2<String, UserVectorContainer> tuple2 : calculateUserSet ) {
				userVectorList.add(tuple2._2);
			}

			userVectorList.sort(new Comparator<UserVectorContainer>() {
				public int compare(UserVectorContainer vec1,UserVectorContainer vec) {

					if(vec1.sim-vec.sim > 0.0){
						return 1;
					}

					else if(vec1.sim-vec.sim == 0.0){
						return 0;
					}

					else{
						return -1;
					}
				}
			});

			Double simNumerator = 0.0;
			Double simDenominator = 0.0;

			System.out.println(userVectorList.size());
			int k =0;
			//boolean valid = false;
			for (UserVectorContainer tuple2 : userVectorList) {				
				double sim  = tuple2.sim;
				double ratings = tuple2.userRatings.get(userId);

				//System.out.println("UserId:"+userId+"Ratings:"+ratings);
                // Ignore negative or Zero similarity ratings
				if(sim <= 0.0){
					continue;
				}

				simNumerator += sim*ratings;
				simDenominator += sim;
				if(k==50){
					break;
				}
				k++;
			}
			String val = simNumerator/simDenominator+"";

			if(simNumerator == 0.0 || simDenominator == 0.0){
				val = "0.0"; 	
			}
			String out ="Rating for Item:"+calItemid+" by userID : "+userId+" is -> "+val+"\n";
			//System.out.println(out);
			RatingList.add(out);
		}
		
		System.out.println(target.userRatings.keySet().size());
		
		for(String user:target.userRatings.keySet()){
			String out ="Rating for Item:"+calItemid+" by userID : "+user+" is -> "+target.userRatings.get(user)+"\n";
			RatingList.add(out);
		}
		
		saveOutput(outputPath,RatingList);
        spark.stop();   	
	}
   
	/**
	 * Save Output. Provide full text path length.
	 * @param outDir
	 * @param list
	 * @throws IOException
	 */
	private static void saveOutput(String outDir,List<String> list) throws IOException {
		File outF = new File(outDir);
		BufferedWriter outFP = new BufferedWriter(new FileWriter(outDir));

		for (String string  : list) {
			outFP.write(string);
		}
		outFP.close();
	}
}
