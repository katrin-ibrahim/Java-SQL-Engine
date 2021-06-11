import java.io.Serializable;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Vector;

@SuppressWarnings("serial")
public class Bucket implements Serializable {

	Hashtable<String,Vector<String>> bucketValues= new Hashtable<String,Vector<String>>();
	String bucketName;
	ArrayList<String> overflowBucketLocations = new ArrayList<>();

	
	
}


