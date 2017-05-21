

import java.util.ArrayList;
import java.util.Arrays;

public class Matrix {
	public double[][] matrix;
	public Matrix(double[][] matrix){
		this.matrix=matrix;
	}
	
	public KVPair[] matrixToCoordTuples(String label){
		ArrayList<KVPair> tuples= new ArrayList<KVPair>();
		for(int i=0;i<this.matrix.length;i++){
			for(int j=0;j<this.matrix[i].length;j++){
				if(matrix[i][j]!=0){
					tuples.add(new KVPair(new Coordinate(label,i,j),matrix[i][j]));
				}
			}
		}
		
		return Arrays.copyOfRange(tuples.toArray(),0,tuples.size(),KVPair[].class);
	}

}
