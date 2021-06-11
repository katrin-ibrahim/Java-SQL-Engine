import java.io.Serializable;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Vector;

public class GridInfo implements Serializable {


//	String gridLoc ;
	String gridName;
	String []colNames;
	Hashtable<String,Object[]> gridRanges = new Hashtable<String,Object[]>(); // ranges for each hashtable entry
//	Vector<String> bucketNames = new Vector<>();
//	Vector<String> bucketNames = new Vector<>();
	Hashtable<String,String> nameToLoc = new Hashtable<>();
	
	
	public GridInfo(String [] colNames){
		this.colNames = colNames;
		
	
}

	
}
