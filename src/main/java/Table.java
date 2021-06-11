import java.io.FileWriter;
import java.io.Serializable;
import java.util.*;

@SuppressWarnings("serial")
public class Table implements Serializable{

	FileWriter csvWriter = null;
	
	String tableName; 
	String clusteringKey; //PRIMARY KEY
	Hashtable<String,String> colNameType ; //CHECKKKKKK TRANSIENTTTTT
	Hashtable<String, String> colNameMin; 
	Hashtable<String, String> colNameMax;
	Page firstPage;
//	int numberOfTuples;
	ArrayList<PageInfo> pageInfoList;
	ArrayList<PageInfo> overflowInfoList;
	
	ArrayList<GridInfo> grids = new ArrayList<>();


	public String getClusteringKey() {
		return clusteringKey;
	}

	public void setClusteringKey(String clusteringKey) {
		this.clusteringKey = clusteringKey;
	}

	public Table(String tableName, String clusteringKey, Hashtable<String, String> colNameType,
			Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) {
		this.tableName=tableName;
		this.clusteringKey=clusteringKey;
		this.colNameType=colNameType;
		this.colNameMin=colNameMin;
		this.colNameMax=colNameMax;
		
		pageInfoList= new ArrayList<PageInfo>();
		overflowInfoList= new ArrayList<PageInfo>();
		
		//firstPage=new Page();
		//firstPage.pageName=tableName + "0" ;
		
		//pageList.add(firstPage);
		//SERIALIZE?????
	}


	public String getTableName() {
		return tableName;
	}
	
//	public int getNumberOfTuples() {
//		return numberOfTuples;
//	}
//
//	public void setNumberOfTuples(int numberOfTuples) {
//		this.numberOfTuples = numberOfTuples;
//	}
	

	public Hashtable<String, String> getColNameType() {
		return colNameType;
	}


}

