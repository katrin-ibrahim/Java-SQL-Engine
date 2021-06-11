import java.io.Serializable;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Vector;

@SuppressWarnings("serial")
public class Page implements Serializable {
	
	Vector<Hashtable<String, Object>> v = new Vector<Hashtable<String, Object>>(); //PAGE=CLASS=VECTOR
	String location; //SRC/MAIN/....PAGE NAME
    String pageName;
	ArrayList<String> overflowLocations = new ArrayList<String>();
	



}
