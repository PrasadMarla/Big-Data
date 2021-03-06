import java.util.concurrent.*;


public class Test {
	public static void main(String[] args) throws ExecutionException, InterruptedException{
		
		KVPair[] data = {new KVPair(1, "The horse raced past the barn fell"),
		                 new KVPair(2, "The complex houses married and single soldiers and their families"),
		                 new KVPair(3, "There is nothing either good or bad, but thinking makes it so"),
		                 new KVPair(4, "I burn, I pine, I perish"),
		                 new KVPair(5, "Come what come may, time and the hour runs through the roughest day"),
		                 new KVPair(6, "Be a yardstick of quality."),
		                 new KVPair(7, "A horse is the projection of peoples' dreams about themselves - strong, powerful, beautiful"),
		                 new KVPair(8, "I believe that at the end of the century the use of words and general educated opinion will have altered so much that one will be able to speak of machines thinking without expecting to be contradicted.")};
		                
		WordCountMR mrObject = new WordCountMR(data,4,3);
		mrObject.runSystem();
		
		double[][] mat={{1,0},{2,3}};
		Matrix k = new Matrix(mat);
		KVPair[] nzs= k.matrixToCoordTuples("mat");
		for(KVPair a : nzs){
			System.out.println("(("+((Coordinate)a.k).label+","+((Coordinate)a.k).row+","+((Coordinate)a.k).col+"),"+a.v.toString()+")");
		}
	}
}
