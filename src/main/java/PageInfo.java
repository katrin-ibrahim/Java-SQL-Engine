import java.io.*;

@SuppressWarnings("serial")
public class PageInfo implements Serializable {
		
	//Vector<Hashtable<String, Object>> overflow = new Vector<Hashtable<String, Object>>(); 

	Object min; //OF PRIMARY KEY
	Object max;
	//String fileName;
	String location; //SRC/MAIN/....PAGE NAME
	String pageName; //ARRAYLIST?????

	
	public PageInfo() {
		
	}
	
	public PageInfo(Object min,Object max) {
		this.min=min;
		this.max=max;
		
	}
	

	
}