package AprioriCSE545.Apriori;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
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
public class App {
	 
	private static SparkSession spark;
	
	private static Dataset<Row> readDataSet(String inputPath) {
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

	private static SparkSession init() {
		
		spark = SparkSession.builder()/*.master("local[8]").*/.appName("ItemSets").getOrCreate();
		
		Configuration hadoopConf = spark.sparkContext().hadoopConfiguration();
		hadoopConf.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem");
		
		
		return spark;
	}
	
	public static void main(String[] args) throws Exception {

		String inputFilePath = args[0];
		String outputFilePath = args[1]; 
		//String outputFilePath = "D:/resultfinal.txt";
		

		List<String> printString = new ArrayList<String>();
		SparkSession spark = init();
		
		/**
		 * Since by default spark has cross join enabled to false.
		 */
		
		spark.conf().set("spark.sql.crossJoin.enabled", true);
		
		/*********************************DataSet Generation Begins*************************/
		
		Dataset<Row> DatasetCSV = readDataSet(inputFilePath);
		//DatasetCSV.count());
		SparkContext sparkContext = spark.sparkContext();
		
		DatasetCSV.createOrReplaceTempView("Dataset");
		Dataset<Row> filteredItems = spark.sql(
				"select item, user from Dataset where item in (select DISTINCT item FROM Dataset group by item having count(item) >= 10)");

		filteredItems.count();

		// filteredItems.printSchema();
		filteredItems.createOrReplaceTempView("filteredStage1");

		Dataset<Row> usersWithMoreThen5Buys = spark
				.sql("select DISTINCT user FROM filteredStage1 GROUP BY user HAVING count(user) >= 5");
		usersWithMoreThen5Buys.persist();
		long basketCount = usersWithMoreThen5Buys.count();
		
		usersWithMoreThen5Buys.createOrReplaceTempView("GroupedUser");
		Dataset<Row> filteredItemStage2 = spark.sql(
				"select filteredStage1.item, filteredStage1.user from filteredStage1, GroupedUser where filteredStage1.user = GroupedUser.user ");

		filteredItemStage2.persist(StorageLevel.MEMORY_AND_DISK_SER());
		filteredItemStage2.count();
		filteredItemStage2.createOrReplaceTempView("BaseDataTable");
		
        /*********************************DataSet Generation Ends**************************************/   
		 
		/**
		  *  Using K-way Joins for K length Pattern Generation
		  */
		/**
		 * One Length Pattern Generation
		 */
		Dataset<Row> oneItemSet = spark.sql("select item, count(*) as count from BaseDataTable group by item");

		oneItemSet.persist(StorageLevel.MEMORY_AND_DISK_SER());
		oneItemSet.createOrReplaceTempView("oneLength");

		/**
		 * Two Length Pattern Generation
		 */
		/**********Tried to merge candidate and frequent item generation in one query but memory gives up************/
	    /*	Dataset<Row> twoItemFrequentPairs = spark
				.sql("select t1.itemid as itemid1, t2.itemid as itemid2, count(*)"
						+ "from oneItemFrequent t1,oneItemFrequent t2, filteredItemstage2 f1,filteredItemstage2 f2"
						+ " where t1.itemid < t2.itemid and t1.itemid = f1.itemid and t2.itemid = f2.itemid and f1.userid = f2.userid"
						+ " group by t1.itemid, t2.itemid"
						+ " having count(*) >= 10");*/
			
		Dataset<Row> twoCandidatePairs = spark.sql(
				"select t1.item as item1, t2.item as item2 from oneLength as t1 cross join oneLength as t2 where t1.item < t2.item");
		twoCandidatePairs.createOrReplaceTempView("twoCandidate");

		Dataset<Row> twolengthFrequent = spark
				.sql("select item1, item2 , count(*) from twoCandidate, BaseDataTable as t1, BaseDataTable as t2"
						+ " where t1.item = twoCandidate.item1 and t2.item = twoCandidate.item2 and t1.user = t2.user"
						+ " group by item1, item2 having count(*) >= 10");

		oneItemSet.unpersist();
    		
		twolengthFrequent.persist(StorageLevel.MEMORY_AND_DISK_SER());
		long twoLengthCOunt = twolengthFrequent.count();
		printString.add("\nTotal number of frequent k=2 pairs are " + twoLengthCOunt );
		twolengthFrequent.createOrReplaceTempView("twoLength");

		/**
		 * Three Length Pattern Generation
		 */

		//Tried to merge candidate and frequent item generation in one query but memory gives up since many tables would be included.
		/*"select t1.itemid1,t1.itemid2,t2.itemid2 as itemid3, count(*) as count3 from twoItemFrequent t1,twoItemFrequent t2, filteredItemstage2 f1,filteredItemstage2 f2,filteredItemstage2 f3 "
		+ " where t1.itemid1 = t2.itemid1 and t1.itemid2 < t2.itemid2 and t1.itemid1 = f1.itemid and t1.itemid2 = f2.itemid and t2.itemid2 = f3.itemid and "
		+ "f1.userid = f2.userid and f2.userid = f3.userid"
		+ " group by t1.itemid1,t1.itemid2,t2.itemid2"
		+ " having count(*) >= 10");//.cache();//persist(StorageLevel.MEMORY_AND_DISK()
       */ 
		
		Dataset<Row> threeCandidatePairs = spark.sql(" select t1.item1 as item1, t1.item2 as item2, t2.item2 as item3 "
				+ "from twoLength as t1, twoLength as t2 " + "where t1.item1 = t2.item1 and t1.item2 < t2.item2");

		threeCandidatePairs.createOrReplaceTempView("threeCandidate");

		Dataset<Row> threeLengthFrequent = spark
				.sql("select item1, item2 , item3, count(*) as count from threeCandidate, BaseDataTable as t1, BaseDataTable as t2, BaseDataTable as t3"
						+ " where t1.item = threeCandidate.item1 and t2.item = threeCandidate.item2 and t3.item = threeCandidate.item3 and t1.user = t2.user and t2.user = t3.user"
						+ " group by item1, item2, item3 having count(*) >= 10");
            
		threeLengthFrequent.persist();
		threeLengthFrequent.createOrReplaceTempView("threeLength");
		twolengthFrequent.unpersist();

		Row[] threeLengthArray = (Row [])threeLengthFrequent.collect();
		final Map<String, Long> threeValues  =  new HashMap<String, Long>();
		
		for (Row row : threeLengthArray) {
			StringBuffer sb = new StringBuffer();
			 sb.append(row.getString(0));
			 sb.append(',');
			 sb.append(row.getString(1));
			 sb.append(',');
			 sb.append(row.getString(2));
			 threeValues.put(sb.toString(),row.getLong(3));
			
		}

		threeLengthArray = null;
		printString.add("\nTotal number of frequent k=3 pairs are " + threeValues.keySet().size() + "\n");
          
		/**
		 * Four Length Pattern Generation
	  	 */
		//Tried to merge candidate and frequent item generation queries  in one query but memory gives up
		/* Combined Query: "select t1.itemid1,t1.itemid2,t1.itemid3,t2.itemid3 as itemid4,count(*) as count4 from filteredItemstage2 f1,filteredItemstage2 f2,filteredItemstage2 f3,filteredItemstage2 f4,threeItemFrequent t1,threeItemFrequent t2 "
			+ " where t1.itemid1 = t2.itemid1 and t1.itemid2 = t2.itemid2 and t1.itemid3 < t2.itemid3 and "
			+ "t1.itemid1 = f1.itemid and t1.itemid2 = f2.itemid and t1.itemid3 = f3.itemid and t2.itemid3 = f4.itemid "
			+ " and f1.userid = f2.userid and f2.userid = f3.userid and f3.userid = f4.userid "
			+ " group by t1.itemid1,t1.itemid2,t1.itemid3,t2.itemid3"
			+ " having count(*) >= 10");//persist(StorageLevel.MEMORY_AND_DISK()
	   */
		
		Dataset<Row> fourCandidatePairs = spark
				.sql("  select t1.item1 as item1, t1.item2 as item2, t1.item3 as item3, t2.item3 as item4 "
						+ "from threeLength as t1, threeLength as t2 "
						+ "where t1.item1 = t2.item1 and t1.item2 = t2.item2 and t1.item3 < t2.item3");
		fourCandidatePairs.createOrReplaceTempView("fourCandidate");
		
		Dataset<Row> fourLengthFrequent = spark.sql("select item1, item2 , item3, item4, count(*) as count "
				+ "from fourCandidate, BaseDataTable as t1, BaseDataTable as t2, BaseDataTable as t3, BaseDataTable as t4"
				+ " where t1.item = fourCandidate.item1 and t2.item = fourCandidate.item2 and t3.item = fourCandidate.item3 and t4.item = fourCandidate.item4 and"
				+ " t1.user = t2.user and t2.user = t3.user and t3.user = t4.user"
				+ " group by item1, item2, item3, item4 having count(*) >= 10");

		
		fourLengthFrequent.persist(StorageLevel.MEMORY_AND_DISK_SER());
		fourLengthFrequent.count();
		fourLengthFrequent.createOrReplaceTempView("fourLengthFrequent");

		threeLengthFrequent.unpersist();
		fourCandidatePairs.unpersist();
		filteredItemStage2.unpersist();

		//System.out.println(outputString);
		
		Row[] fourItemFrequentPairslist = (Row[]) fourLengthFrequent.collect();
		printString.add("\nTotal number of frequent k=4 pairs are " + fourItemFrequentPairslist.length + "\n");  
		JavaPairRDD<String, Long> oneItemSupportRDD = spark
				.sql("select oneLength.item, oneLength.count from oneLength, fourLengthFrequent where oneLength.item = fourLengthFrequent.item4 OR oneLength.item = fourLengthFrequent.item3 OR oneLength.item = fourLengthFrequent.item2 OR oneLength.item = fourLengthFrequent.item1").javaRDD()
				.mapToPair(new PairFunction<Row, String, Long>() {
					@Override
					public Tuple2<String, Long> call(Row arg0) throws Exception {
						return new Tuple2<String, Long>(arg0.getString(0),arg0.getLong(1));					
					}
				});
		
		Map<String, Long> oneValues = oneItemSupportRDD.collectAsMap();
		
	    fourLengthFrequent.unpersist();
		
	    /***
	     * Confidence and Interest Pruning
	     */
	    for(int i =0;i<fourItemFrequentPairslist.length;i++){
			Row arg0 = fourItemFrequentPairslist[i]; 
			double fourLength =  arg0.getLong(4);
			double threelength1 = threeValues.get(arg0.getString(0)+","+arg0.getString(1)+","+arg0.getString(2));
			double threelength2 = threeValues.get(arg0.getString(1)+","+arg0.getString(2)+","+arg0.getString(3));
			double threelength3 = threeValues.get(arg0.getString(0)+","+arg0.getString(2)+","+arg0.getString(3));
			double threelength4 = threeValues.get(arg0.getString(0)+","+arg0.getString(1)+","+arg0.getString(3));
			double oneLength1 = oneValues.get(arg0.getString(3));
			double oneLength2 = oneValues.get(arg0.getString(0));
			double oneLength3 = oneValues.get(arg0.getString(1));
			double oneLength4 = oneValues.get(arg0.getString(2));
			double cs1 = fourLength/threelength1;
			double cs2 = fourLength/threelength2;
			double cs3 = fourLength/threelength3;
			double cs4 = fourLength/threelength4;
			double support1 = (double) (cs1 - (oneLength1/basketCount));
			double support2 = (double) (cs2 - (oneLength2/basketCount));
			double support3 = (double) (cs3 - (oneLength3/basketCount));
			double support4 = (double) (cs4 - (oneLength4/basketCount));
			List<String> rows = new ArrayList<String>();
			if(cs1>0.05 && support1>0.02){
				String pattern =  "{Association Rule : "+arg0.getString(0)+","+arg0.getString(1)+","+arg0.getString(2)+"}"+"->"+arg0.getString(3)+")";/*+" with confidence : "+cs1+" and support : "+support1*/ ;
				//System.out.println(pattern);
				printString.add(pattern);
				printString.add("\n");
			}
			if(cs2>0.05 && support2>0.02){
				String pattern =  "{ Association Rule : "+arg0.getString(1)+","+arg0.getString(2)+","+arg0.getString(3)+"}"+"->"+arg0.getString(0)+" with confidence : "+cs2+" and support : "+support2;
				//System.out.println(pattern);
				printString.add(pattern);
				printString.add("\n");
			}
			if(cs3>0.05 && support3>0.02){
				String pattern =  "{ Association Rule : "+arg0.getString(0)+","+arg0.getString(2)+","+arg0.getString(3)+"}"+"->"+arg0.getString(1)+" with confidence : "+cs3+" and support : "+support3;
				//System.out.println(pattern);
				printString.add(pattern);
				printString.add("\n");
			}
			if(cs4>0.05 && support4>0.02){
				String pattern =  "{ Association Rule : "+arg0.getString(0)+","+arg0.getString(1)+","+arg0.getString(3)+"}"+"->"+arg0.getString(2)+" with confidence : "+cs4+" and support : "+support4;
				//System.out.println(pattern);
				printString.add(pattern);
				printString.add("\n");
			}
			//rows.add("\n");
			printString.add("\n");
		  }
		
		saveOutput(printString,outputFilePath);
	    spark.stop();
	}

	/**
	 * File Writer
	 * @param output
	 * @param outDir
	 * @throws IOException
	 */
	private static void saveOutput(List<String> output, String outDir) throws IOException {

		File outF = new File(outDir);
		//outF.mkdirs();
		BufferedWriter outFP = new BufferedWriter(new FileWriter(outDir));

		for (String str : output) {
			outFP.write(str);
		}		
		outFP.close();

	}

}