import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.io.*;

@SuppressWarnings("serial")
public class DBApp implements DBAppInterface, Serializable {
	int maxRows;
	int maxBucketCount;

	FileWriter csvWriter = null;
	Writer writer = null;

	ArrayList<String> tableNames = new ArrayList<String>();


	public void init() {

		File dataDirectory = new File("src/main/resources/data");

		if (!dataDirectory.exists()) {
			dataDirectory.mkdirs();
		}

		Properties p = new Properties();
		String fileName = "src/main/resources/DBApp.config";
		InputStream i = null;
		try {
			i = new FileInputStream(fileName);

		} catch (FileNotFoundException ex) {
			ex.printStackTrace();

		}
		try {
			p.load(i);
			maxRows = Integer.parseInt(p.getProperty("MaximumRowsCountinPage"));
			// maxRows=5;
			// System.out.println(maxRows);
			maxBucketCount = Integer.parseInt(p.getProperty("MaximumKeysCountinIndexBucket"));
		} catch (IOException ex) {
			System.out.println("io caught");
		}

		try {

			// csvWriter = new FileWriter("src/main/resources/metadata.csv");
			csvWriter = new FileWriter("src/main/resources/metadata.csv", true);

		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		try {
			csvWriter.append("Table Name,Column Name,Column Type,ClusteringKey,Indexed,min,max");
			csvWriter.append("\n");
			csvWriter.flush();
			csvWriter.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// System.out.println("meta data created");
		// System.out.println(csvWriter);

	}

	public void createTable(String tableName, String clusteringKey, Hashtable<String, String> colNameType,
			Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) throws DBAppException {

		if (tableNames.contains(tableName))
			throw new DBAppException("A table already exists with this name");

		ArrayList<String> typesAllowed = new ArrayList<String>() {
			{
				add("java.lang.Integer");
				add("java.lang.integer");
				add("java.lang.String");
				add("java.lang.string");
				add("java.lang.Double");
				add("java.lang.double");
				add("java.util.Date");
				add("java.util.date");
			}
		};

		Enumeration<String> e = colNameType.keys();
		while (e.hasMoreElements()) {
			String key = e.nextElement();
			if (!(typesAllowed.contains(colNameType.get(key))))
				throw new DBAppException("incorrect data types entered for table creation");
		}
		
		tableNames.add(tableName);
		Table t = new Table(tableName, clusteringKey, colNameType, colNameMin, colNameMax);
		System.out.println(t.tableName + " TABLE CREATED");

		serialize(t);
		writeToCSV(t);

	}

	public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {
		BufferedReader br = null;
		ArrayList<String> columnNames = new ArrayList<String>();
		ArrayList<String> indexedColumns = new ArrayList<>();

		try {
			br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		String current = null;
		try {
			// ???
			current = br.readLine();
			//current = br.readLine(); // CHECKKK

		} catch (IOException e) {
			e.printStackTrace();
		}

		while (current != null) {
			String[] line = current.split(",");
			String tName = line[0];
			String colName = line[1];
			String colType = line[2];

			@SuppressWarnings("unused")
			String clusteringKey = line[3];
			// do we need tname ?

			String indexed = line[4];
			if(indexed.equals("True")){
				indexedColumns.add(colName);
			}
			Object min = line[5];
			Object max = line[6];

			// System.out.println(min);
			// System.out.println(max);

			if (tName.equals(tableName)) {
				columnNames.add(colName);
			}

			Enumeration<String> colNameValueE = colNameValue.keys();
			while (colNameValueE.hasMoreElements()) {
				String key = colNameValueE.nextElement();
				String s = colNameValue.get(key).getClass() + "";
				if (key.equals(colName) && !(s.substring(6).equalsIgnoreCase(colType))) {
					// System.out.println(key);
					// System.out.println(colName);
					// System.out.println(colType);
					throw new DBAppException("INCORRECT DATA TYPE");
				}
			}

			Enumeration<String> e10 = colNameValue.keys();
			while (e10.hasMoreElements()) {
				String key = e10.nextElement();
				String s = colNameValue.get(key) + "";
				// System.out.println(s);
				// System.out.println(s);
				if (key.equalsIgnoreCase(colName)) {
					// System.out.println(colType);
					if (colType.equalsIgnoreCase("java.lang.Integer")) {

						@SuppressWarnings("deprecation")
						Integer minV = new Integer(min.toString());
						@SuppressWarnings("deprecation")
						Integer maxV = new Integer(max.toString());

						if ((Integer.parseInt(s)) < minV.intValue()) {
							throw new DBAppException("CAN'T INSERT. VALUE LESS THAN THE ALLOWED MINIMUM");

						}

						if ((Integer.parseInt(s)) > maxV.intValue()) {
							throw new DBAppException("CAN'T INSERT. VALUE GREATER THAN THE ALLOWED MAXIMUM");

						}

					}

					else if (colType.equalsIgnoreCase("java.lang.Double")) {

						double d = Double.parseDouble(s);

						@SuppressWarnings("deprecation")
						Double minV = new Double(min.toString());
						@SuppressWarnings("deprecation")
						Double maxV = new Double(max.toString());

						if (Double.compare(d, minV.doubleValue()) < 0) {
							throw new DBAppException("CAN'T INSERT. VALUE LESS THAN THE ALLOWED MINIMUM");

						}

						if (Double.compare(d, maxV.doubleValue()) > 0) {
							throw new DBAppException("CAN'T INSERT. VALUE GREATER THAN THE ALLOWED MAXIMUM");

						}
						// (Double.compare((double)s, (Double)max)>0 )

					}

					else if (colType.equalsIgnoreCase("java.lang.String")) {

						if ((s).compareTo(min + "") < 0) {
							throw new DBAppException("CAN'T INSERT. VALUE LESS THAN THE ALLOWED MINIMUM");
						}

						if ((s).compareTo(max + "") > 0) {
							throw new DBAppException("CAN'T INSERT. VALUE GREATER THAN THE ALLOWED MAXIMUM");
						}

					} else if (colType.equalsIgnoreCase("java.util.Date")) {

						// System.out.println("DATEEEE"+s);
						String date = getDate(s);
						// System.out.println(d);

						if (date.compareTo(min + "") < 0) {
							throw new DBAppException("CAN'T INSERT. VALUE LESS THAN THE ALLOWED MINIMUM");

						}

						if (date.compareTo(max + "") > 0) {
							throw new DBAppException("CAN'T INSERT. VALUE GREATER THAN THE ALLOWED MAXIMUM");

						}

					}

				}

			}

			try {
				current = br.readLine();
			} catch (IOException e) {

				e.printStackTrace();
			}
		}

		try {
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		// System.out.println(tables.size()+"AAAAAAAAA");

		Table insertTable = (Table) deserialize("src/main/resources/data/" + tableName + ".ser");
		// System.out.println(insertTable.tableName);
		if (insertTable == null) {
			throw new DBAppException("table doesn't exist");

		}

		if (!(colNameValue.containsKey(insertTable.clusteringKey))) {
			throw new DBAppException("PLEASE INSERT PRIMARY KEY");
		}

		// System.out.println(columnNames);
		// System.out.println("BBBBBBBBB");
		Enumeration<String> enumerator = colNameValue.keys();
		while (enumerator.hasMoreElements()) {
			String key = enumerator.nextElement();

			if (!(columnNames.contains(key))) {
				throw new DBAppException("YOU ARE TRYING TO INSERT IN A COLUMN THAT DOESN'T EXIST");

			}
		}


//		Enumeration<String> e2 = colNameValue.keys();
//		ArrayList<String> keyColNames = new ArrayList<String>();
//		while(e2.hasMoreElements()){
//			String k = e2.nextElement();
//			keyColNames.add(k); //name,age
//		}
//
//
//		Collections.sort(keyColNames);
		GridInfo gr = null;
//
//		for(int p = 0 ;p< insertTable.grids.size();p++){
//			Arrays.sort(insertTable.grids.get(p).colNames);
//			String []tmp = insertTable.grids.get(p).colNames;
//			Arrays.sort(tmp);
//			if(Arrays.asList(tmp).equals(keyColNames)){//checking if index exists on columns
//				gr = insertTable.grids.get(p);
//				break;
//			}
//		}


		gr=findGrid(insertTable,colNameValue);
		
		// finding table to insert into

		if (insertTable.pageInfoList.isEmpty()) { // 1st insert in whole table(no pages)
			Page p = new Page();
			p.v.add(0, colNameValue);

			p.pageName = insertTable.tableName + "1";


			p.location = serialize(p); // ----------------


			PageInfo pInfo = new PageInfo();

			pInfo.pageName = p.pageName;
			pInfo.location = p.location;

			String clusteringKey = insertTable.clusteringKey;
			// System.out.println(clusteringKey);

			Enumeration<String> en = colNameValue.keys();
			while (en.hasMoreElements()) {
				String key = en.nextElement();

				if (key.equals(clusteringKey)) {
					pInfo.min = colNameValue.get(key);
					pInfo.max = colNameValue.get(key);
					break;
				}
			}


			insertTable.pageInfoList.add(pInfo);
			insertTable.pageInfoList.size();
			adjustMinMax(insertTable, p, insertTable.pageInfoList.get(0).location);
			serialize(p);
			
			if(gr!=null){
				insertInBucket(tableName,p.location,gr,colNameValue);
			}


		}

		else if ((insertTable.pageInfoList.size() == 1)) { // insert into first page
			// System.out.println("INSERTING INTO FIRST PAGE");
			Page firstPage = (Page) deserialize(insertTable.pageInfoList.get(0).location);
			// System.out.println(firstPage.v);
			Enumeration<String> eVal = colNameValue.keys();
			Object pk = null;
			while (eVal.hasMoreElements()) {
				String key = eVal.nextElement();
				if (insertTable.clusteringKey.equals(key)) {
					pk = colNameValue.get(key);
					break;
				}
			}

			// System.out.println(pk);

			Object currentV = null;
			for (int y = 0; y < firstPage.v.size(); y++) {
				Hashtable<String, Object> currentRecord = firstPage.v.get(y);

				Enumeration<String> e = currentRecord.keys();
				while (e.hasMoreElements()) {
					String key = e.nextElement();
					if (key.equals(insertTable.clusteringKey)) {
						currentV = currentRecord.get(key);
						break;
					}

				}

				if (currentV.equals(pk)) {
					serialize(firstPage);
					throw new DBAppException("CAN'T INSERT.PRIMARY KEY ALREADY EXISTS");
				}

			}

			if ((firstPage.v.size() < maxRows)) {
				int insertAt = binarySearchPageInsert(firstPage.v, pk, insertTable.clusteringKey);


				firstPage.v.insertElementAt(colNameValue, insertAt);


				adjustMinMax(insertTable, firstPage, insertTable.pageInfoList.get(0).location);

				// SERIALIZATION


				serialize(firstPage);
				if(gr!=null){
					insertInBucket(tableName,firstPage.location,gr,colNameValue);
				}

			} else { // 1st entry in 2nd page


				int insertAt = binarySearchPageInsert(firstPage.v, pk, insertTable.clusteringKey);

				// System.out.println(insertAt);
				firstPage.v.insertElementAt(colNameValue, insertAt);

				Page secondPage = new Page();
				secondPage.pageName = insertTable.tableName + (insertTable.pageInfoList.size() + 1);

				Hashtable<String, Object> lastRecord = firstPage.v.lastElement();





				secondPage.v.add(lastRecord);
				firstPage.v.remove(lastRecord);


				PageInfo pInfo = new PageInfo();

				pInfo.pageName = secondPage.pageName;

				@SuppressWarnings("unused")
				String clusteringKey = insertTable.clusteringKey;


				Enumeration<String> en = secondPage.v.get(0).keys();
				while (en.hasMoreElements()) {
					String key = en.nextElement();

					if (key.equals(insertTable.clusteringKey)) {
						pInfo.min = secondPage.v.get(0).get(key);
						pInfo.max = secondPage.v.get(0).get(key);
						break;
					}
				}



				String loc = serialize(secondPage);




				secondPage.location = loc;
				pInfo.location = loc;

				insertTable.pageInfoList.add(pInfo);

				adjustMinMax(insertTable, firstPage, insertTable.pageInfoList.get(0).location);
				serialize(firstPage);
				// serialize(secondPage);

				if(gr!=null){
					if(lastRecord.equals(colNameValue)){
						insertInBucket(tableName,secondPage.location,gr,lastRecord);
					}
					else{
						//update pointer
						deleteFromBucket(tableName,gr,lastRecord);
						insertInBucket(tableName,secondPage.location,gr,lastRecord);
						insertInBucket(tableName,firstPage.location,gr,colNameValue);
					}


				}



			}

		} // LAW PAGELIST.SIZE NOT EQUALS 1
		else { // AT LEAST 2 PAGES

			insertHelper(insertTable, colNameValue,gr);

		}

		// END OF CAN INSERT

		System.out.println("INSERTED SUCCESSFULLY IN TABLE: " + tableName);


		serialize(insertTable);

	}

	//call inside insert with correct locations
	public void insertInBucket (String tableName ,String location , GridInfo gInfo,Hashtable<String ,Object> insertedRecord){

		Enumeration<String> colNameValueE = insertedRecord.keys();
		Enumeration<String> columns = gInfo.gridRanges.keys();		

		ArrayList<Integer> ranges = new ArrayList<>();
		String s = "";
		int cellIndex=0;

		
		while(columns.hasMoreElements()) { //name.id
			String key2 = columns.nextElement();
						
			colNameValueE = insertedRecord.keys();
			
			while (colNameValueE.hasMoreElements()) {
				String key = colNameValueE.nextElement();
				if(key.equals(key2)) {
					s = s + insertedRecord.get(key);
					s+=",";
				}

			}
		 }
		
		s=s.substring(0, s.length()-1); 
		
		colNameValueE = insertedRecord.keys();

		while (colNameValueE.hasMoreElements()) {
				String key = colNameValueE.nextElement();
				if(gInfo.gridRanges.containsKey(key)) {
					String st="";
					String d="";
					if (insertedRecord.get(key) instanceof String) { //id for example

						if((insertedRecord.get(key)+"").charAt(0)>=49  
								&& (insertedRecord.get(key)+"").charAt(0)<=57) {
							 	String[] temp = (insertedRecord.get(key)+"").split("-");
							 	
				                st = temp[0]+temp[1];
				                
								if(gInfo.gridRanges.get(key)[gInfo.gridRanges.get(key).length-1]==null) {
									cellIndex = Arrays.binarySearch(gInfo.gridRanges.get(key), 0, gInfo.gridRanges.get(key).length-1, Integer.parseInt(st));

								}
								else {
									cellIndex = Arrays.binarySearch(gInfo.gridRanges.get(key), 0, gInfo.gridRanges.get(key).length, Integer.parseInt(st));
									
								}
				                
								
						}
						
						else {
							
							if(gInfo.gridRanges.get(key)[gInfo.gridRanges.get(key).length-1]==null) {
								cellIndex = Arrays.binarySearch(gInfo.gridRanges.get(key), 0, gInfo.gridRanges.get(key).length-1, ((String)insertedRecord.get(key)).charAt(0));

							}
							else {
								cellIndex = Arrays.binarySearch(gInfo.gridRanges.get(key), 0, gInfo.gridRanges.get(key).length, ((String)insertedRecord.get(key)).charAt(0));
								
							}
							
						}
					
					}
					
					else if(insertedRecord.get(key) instanceof Date) {
						d=insertedRecord.get(key)+"";
						
						String str=getDate(d);
						
						DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

						LocalDate localDate = LocalDate.parse(str, formatter);
						
						
						if(gInfo.gridRanges.get(key)[gInfo.gridRanges.get(key).length-1]==null) {
							cellIndex = Arrays.binarySearch(gInfo.gridRanges.get(key), 0, gInfo.gridRanges.get(key).length-1, localDate);

						}
						else {
							cellIndex = Arrays.binarySearch(gInfo.gridRanges.get(key), 0, gInfo.gridRanges.get(key).length, localDate);
							
						}
						

					}
					
					else {
						
						if(gInfo.gridRanges.get(key)[gInfo.gridRanges.get(key).length-1]==null) {
							cellIndex = Arrays.binarySearch(gInfo.gridRanges.get(key), 0, gInfo.gridRanges.get(key).length-1, insertedRecord.get(key));

						}
						else {
							cellIndex = Arrays.binarySearch(gInfo.gridRanges.get(key), 0, gInfo.gridRanges.get(key).length, insertedRecord.get(key));
							
						}

						//System.out.println(cellIndex);
						//System.out.println("EVERYTHING IS FINEEEE");
					}
					
					
					if (cellIndex < 0) {
						cellIndex = (cellIndex * (-1)) - 2;
					}
					ranges.add(cellIndex);
//					bucketIndex+=cellIndex;
//				}
				
			
		  }
		}
		
		//System.out.println(ranges);
		String bucketName = getBucketName(tableName,gInfo.colNames,ranges);
		boolean found = false;


		String bucketLoc = gInfo.nameToLoc.get(bucketName);
		if(bucketLoc!=null){
			found = true;
		}


		if (found) {
			Bucket b13 = (Bucket) deserialize(bucketLoc);
//			Bucket b13 = (Bucket) deserialize(gInfo.nameToLoc.get(bucketName));
			if (b13.bucketValues.size() < maxBucketCount) {//found bucket and there is space in it
				if(b13.bucketValues.contains(s)) {
					b13.bucketValues.get(s).add(location); //"10,20,30"

				}
				else {
					Vector<String> v = new Vector<String>();
					v.add(location);
					b13.bucketValues.put(s, v);

				}
				serialize(b13);

			} else { // no space in bucket so check overflows -> overflows empty so create
				if (b13.overflowBucketLocations.isEmpty()) {
					Bucket overflow = new Bucket();
					
					Vector<String> v = new Vector<String>();
					v.add(location);
					
					overflow.bucketValues.put(s, v);
					//student#name0,age1|1
					overflow.bucketName = b13.bucketName + "#" + b13.overflowBucketLocations.size();
					b13.overflowBucketLocations.add(serialize(overflow));

				} else { //there is overflow so deserialize and check size
					int index = (b13.overflowBucketLocations.size() - 1);
					Bucket overflow = (Bucket) deserialize(b13.overflowBucketLocations.get(index));
					
					if (overflow.bucketValues.size() < maxBucketCount) { //-> there is room
						//overflow.bucketValues.put(s, location);
						
						if(overflow.bucketValues.contains(s)) {
							overflow.bucketValues.get(s).add(location); //"10,20,30"

						}
						else {
							Vector<String> v = new Vector<String>();
							v.add(location);
							overflow.bucketValues.put(s, v);

						}
						
					} else {//->last overflow bucket is full so create new one
						Bucket overflow1 = new Bucket();
						
						
						//overflow1.bucketValues.put(s, location);
						
						Vector<String> v = new Vector<String>();
						v.add(location);
						overflow1.bucketValues.put(s, v);
						
						
						overflow1.bucketName = b13.bucketName + "#" + b13.overflowBucketLocations.size();
						b13.overflowBucketLocations.add(serialize(overflow));
					}
				}

			}
		}

		//add ser regular buckets to vector in gridInfo??

		else { // no bucket found -> so create new bucket
			//bucketinsertduplicatesInHash;

			Bucket b13 = new Bucket();

			Vector<String> v = new Vector<String>();
			v.add(location);
			b13.bucketValues.put(s, v);
			
			b13.bucketName = bucketName;
//			gInfo.bucketNames.add(indexBinSearch,bucketIndex);
//			gInfo.bucketNames.add(bucketName);
			gInfo.nameToLoc.put(b13.bucketName,serialize(b13));

		}

	}

	//will return a list of bucketLocations

	//bucket #col0,indexCol1 #col1,indexCol2#... -> to get partials binary search 3la el 7agat eli indexed to find their index then concat to colName and add(col0,indexCol1)
	//within bucket we have s which is values -> add (5) add(ahmed) add(2.0)
	//so within bucket split s on "," to get values in array then use containsAll
	//tname#col,indexBinSearch#.....
	//ahmed ,  25
	//name,7 #
	public ArrayList<Vector<String>> getPartialQuery(GridInfo gInfo,ArrayList<String> partial,ArrayList<String>values){
		ArrayList<String> bucketLocations = new ArrayList<>();
		ArrayList<Vector<String>> pageLocations = new ArrayList<Vector<String>>();
		Enumeration<String> e = gInfo.nameToLoc.keys();
		//loop on all buckets
		while (e.hasMoreElements()){
			String key = e.nextElement();
			//split bucket name on #
			String [] bucketContent = key.split("#");
			if(Arrays.asList(bucketContent).containsAll(partial)){
				bucketLocations.add(gInfo.nameToLoc.get(key));
			}

		}
		//in bucket or overflow

		//loop on buckets matching ranges
		for(int i = 0 ; i < bucketLocations.size();i++){
			Bucket b = (Bucket)deserialize(bucketLocations.get(i));

			Enumeration<String> e2 = b.bucketValues.keys();
			while (e.hasMoreElements()){
				String key = e2.nextElement();
				String [] valueSplit = key.split(",");
				if(Arrays.asList(valueSplit).containsAll(values)){
					pageLocations.add(b.bucketValues.get(key));
				}
			}

			for(int j = 0 ; j < b.overflowBucketLocations.size();j++){
				Bucket overflow = (Bucket)deserialize(b.overflowBucketLocations.get(j));
				Enumeration<String> e3 = overflow.bucketValues.keys();
				while (e.hasMoreElements()){
					String key = e3.nextElement();
					String [] valueSplit = key.split(",");
					if(Arrays.asList(valueSplit).containsAll(values)){
						pageLocations.add(overflow.bucketValues.get(key));
					}
				}
			}

			//loop on one bucket


		}
		return pageLocations;
	}
	//partial res  , page1 , page2
	//B0 id5 email123 nameahmed , B1 id6 email712 nameKarim
	//partial id5 nameAhmed


	public Vector<String> deleteFromBucket(String tableName , GridInfo gInfo,Hashtable<String ,Object> deletedRecord){

		Enumeration<String> colNameValueE = deletedRecord.keys();
		ArrayList<Integer> ranges = new ArrayList<>();
		String s = "";
		int cellIndex;
		
		while (colNameValueE.hasMoreElements()) {
			String key = colNameValueE.nextElement();

			for(int i = 0 ; i<gInfo.colNames.length;i++){
				System.out.println(gInfo.colNames[i]);
				System.out.println("KEY: ");
				if(gInfo.colNames[i].equals(key)){
					System.out.println("KEY:"+gInfo.colNames[i]);
					s = s + deletedRecord.get(key);
					if(i != gInfo.colNames.length - 1)
						s+= ","; //search with s
					//cellIndex = Arrays.binarySearch(gInfo.gridRanges.get(key), 0, gInfo.gridRanges.get(key).length, deletedRecord.get(key));
					
					String st="";
					if (deletedRecord.get(key) instanceof String) { //id for example

						if((deletedRecord.get(key)+"").charAt(0)>=49  
								&& (deletedRecord.get(key)+"").charAt(0)<=57) {
							 	String[] temp = (deletedRecord.get(key)+"").split("-");
							 	
				                st = temp[0]+temp[1];
				                
								if(gInfo.gridRanges.get(key)[gInfo.gridRanges.get(key).length-1]==null) {
									cellIndex = Arrays.binarySearch(gInfo.gridRanges.get(key), 0, gInfo.gridRanges.get(key).length-1, Integer.parseInt(st));

								}
								else {
									cellIndex = Arrays.binarySearch(gInfo.gridRanges.get(key), 0, gInfo.gridRanges.get(key).length, Integer.parseInt(st));
									
								}
				                
								
						}
						
						else {
							
							if(gInfo.gridRanges.get(key)[gInfo.gridRanges.get(key).length-1]==null) {
								cellIndex = Arrays.binarySearch(gInfo.gridRanges.get(key), 0, gInfo.gridRanges.get(key).length-1, ((String)deletedRecord.get(key)).charAt(0));

							}
							else {
								cellIndex = Arrays.binarySearch(gInfo.gridRanges.get(key), 0, gInfo.gridRanges.get(key).length, ((String)deletedRecord.get(key)).charAt(0));
								
							}
							
						}
					
					}
					
					else if(deletedRecord.get(key) instanceof Date) {
						String d=deletedRecord.get(key)+"";
						
						String str=getDate(d);
						
						DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

						LocalDate localDate = LocalDate.parse(str, formatter);
						
						
						if(gInfo.gridRanges.get(key)[gInfo.gridRanges.get(key).length-1]==null) {
							cellIndex = Arrays.binarySearch(gInfo.gridRanges.get(key), 0, gInfo.gridRanges.get(key).length-1, localDate);

						}
						else {
							cellIndex = Arrays.binarySearch(gInfo.gridRanges.get(key), 0, gInfo.gridRanges.get(key).length, localDate);
							
						}
						

					}
					
					else {
						
						if(gInfo.gridRanges.get(key)[gInfo.gridRanges.get(key).length-1]==null) {
							cellIndex = Arrays.binarySearch(gInfo.gridRanges.get(key), 0, gInfo.gridRanges.get(key).length-1, deletedRecord.get(key));

						}
						else {
							cellIndex = Arrays.binarySearch(gInfo.gridRanges.get(key), 0, gInfo.gridRanges.get(key).length, deletedRecord.get(key));
							
						}

						//System.out.println(cellIndex);
						//System.out.println("EVERYTHING IS FINEEEE");
					}
					
					
					if (cellIndex < 0) {
						cellIndex = (cellIndex * (-1)) - 2;
					}
					ranges.add(cellIndex);
				}
			}

		}

		System.out.println(ranges);
		String bucketName = getBucketName(tableName,gInfo.colNames,ranges);
		Bucket b = (Bucket)deserialize(gInfo.nameToLoc.get(bucketName));

		Vector<String> pagesToDeleteFrom = b.bucketValues.get(s);
		if (pagesToDeleteFrom!=null){
			b.bucketValues.remove(s);
		}
		else{
			for(int i = 0 ; i<b.overflowBucketLocations.size();i++) {
				Bucket overflow = (Bucket) deserialize(b.overflowBucketLocations.get(i));
				pagesToDeleteFrom = overflow.bucketValues.get(s);
				if(pagesToDeleteFrom!=null){
					overflow.bucketValues.remove(s);
					break;
				}
			}
		}

		return pagesToDeleteFrom;
	}

	public String getDate(String s) {
		String[] sp = s.trim().split("\\s+");

		String month = null;

		switch (sp[1]) {
		case "Jan":
			month = "01";
			break;
		case "Feb":
			month = "02";
			break;
		case "Mar":
			month = "03";
			break;
		case "Apr":
			month = "04";
			break;
		case "May":
			month = "05";
			break;
		case "Jun":
			month = "06";
			break;
		case "Jul":
			month = "07";
			break;
		case "Aug":
			month = "08";
			break;
		case "Sep":
			month = "09";
			break;
		case "Oct":
			month = "10";
			break;
		case "Nov":
			month = "11";
			break;
		case "Dec":
			month = "12";
			break;

		}

		String x = sp[5] + "-" + month + "-" + sp[2];

		// SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		// return null;

		return x;

	}

	public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue)
			throws DBAppException {

		ArrayList<String> keyColNames = new ArrayList<String>();
		GridInfo gInfo = null;

		Table updateFromTable = (Table) deserialize("src/main/resources/data/" + tableName + ".ser");
		if (updateFromTable == null) {
			throw new DBAppException("table doesn't exist");
		}

		// System.out.println(updateFromTable.tableName+"--");

		Enumeration<String> enumerator = columnNameValue.keys();
		while (enumerator.hasMoreElements()) {
			String key = enumerator.nextElement();

			if (!(updateFromTable.colNameType.keySet().contains(key))) {
				throw new DBAppException("YOU ARE TRYING TO UPDATE VALUE FOR A COLUMN THAT DOESN'T EXIST");

			}
		}
		
		
//		Enumeration<String> e2 = columnNameValue.keys();
//		while(e2.hasMoreElements()){
//			String k = e2.nextElement();
//			keyColNames.add(k); //name,age
//		}
//		
//		Collections.sort(keyColNames);
//		for(int i = 0 ; i<updateFromTable.grids.size();i++){
//			String []tmp = updateFromTable.grids.get(i).colNames;
//			Arrays.sort(tmp);
//			if(Arrays.asList(tmp).equals(keyColNames)){
//				gInfo = updateFromTable.grids.get(i);
//				break;
//			}
//		}
		
		Hashtable<String,Object> h=new Hashtable<String,Object>();
		
		if(updateFromTable.colNameType.get(updateFromTable.clusteringKey).equalsIgnoreCase("java.util.date")) {
			DateFormat format11 = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
			Date pkDate = null;
			try {
				pkDate = format11.parse(clusteringKeyValue);
			} catch (ParseException e) {
				e.printStackTrace();
			} 
			
			
			h.put(updateFromTable.clusteringKey, pkDate);
		}
		
		else {
			h.put(updateFromTable.clusteringKey, clusteringKeyValue);
		}

		
		Enumeration<String> en = columnNameValue.keys();
		while (en.hasMoreElements()) {
			String key = en.nextElement();
			h.put(key, columnNameValue.get(key));
		}

		gInfo=findGrid(updateFromTable,h);
		System.out.println("GRID INFO"+gInfo);
		
		if(gInfo!=null){//find bucket
			String s = "";
			ArrayList <Integer> ranges = new ArrayList<>();
			int cellIndex;
			Vector<String> pagesToUpdate = deleteFromBucket(tableName,gInfo,h);
			if(pagesToUpdate==null){
				throw new DBAppException("YOU ARE TRYING TO DELETE A RECORD THAT DOESN'T EXIST");
			}
			
			Page pageToUpdate= (Page)deserialize(pagesToUpdate.get(0));
			
			String type= updateFromTable.colNameType.get(updateFromTable.clusteringKey);
			
			int recordIndex;
			
			Object pk = null;
			
			switch(type) {
			
				case "java.lang.Integer":  pk=Integer.parseInt(clusteringKeyValue); break;
				case "java.lang.Double":   pk=Double.parseDouble(clusteringKeyValue); break;
				case "java.lang.String":   pk= (String) clusteringKeyValue; break;
				case "java.util.Date": 	   DateFormat format10 = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
											try {
												pk = format10.parse(clusteringKeyValue);
											} catch (ParseException e) {
												e.printStackTrace();
											}  break;
			
			
				}
			
			
			recordIndex = binarySearchPage(pageToUpdate.v, pk , updateFromTable.clusteringKey);
			
			Hashtable<String, Object> record = pageToUpdate.v.get(recordIndex);

			record.forEach((key1, value1) -> {
				columnNameValue.forEach((key2, value2) -> {
					if (key1.equals(key2))
						record.replace(key1, value2);
				});
			});
		
			
			insertInBucket(tableName,pageToUpdate.location,gInfo,record);

			serialize(pageToUpdate);
			
		}
		
		else { //NO INDEX

		Object primaryKey = null;
		String type = null;
		// String type= ""+ clusteringKeyValue.getClass();
		// System.out.println(type);
		String location;
		Page p = null;
		int recordIndex = -1;
		// System.out.println(type.substring(6).toLowerCase());

		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		String current = null;
		try {
			// ???
			current = br.readLine();
			current = br.readLine(); // CHECKKK

		} catch (IOException e) {
			e.printStackTrace();
		}

		while (current != null) {
			String[] line = current.split(",");
			String tName = line[0];
			String colName = line[1];
			String colType = line[2];

			@SuppressWarnings("unused")
			String clusteringKey = line[3];

			if (tName.equals(updateFromTable.tableName) && colName.equals(updateFromTable.clusteringKey)) {
				type = colType.toLowerCase();
				// System.out.println(type);
			}

			try {
				current = br.readLine();
			} catch (IOException e) {

				e.printStackTrace();
			}
		}

		try {
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		switch (type) {
		case ("java.lang.integer"):
			location = binarySearchInt(updateFromTable.pageInfoList, 0, updateFromTable.pageInfoList.size() - 1,
					Integer.parseInt(clusteringKeyValue));
			p = (Page) deserialize(location);
			recordIndex = binarySearchPage(p.v, Integer.parseInt(clusteringKeyValue), updateFromTable.clusteringKey);

			primaryKey = Integer.parseInt(clusteringKeyValue);
			System.out.println(primaryKey);
			System.out.println(recordIndex + "---------");

			break;

		case ("java.lang.string"):
			// System.out.println("BEFORE BINARY SEARCH");
			// System.out.println(updateFromTable.pageInfoList.get(0).min);
			location = binarySearchString(updateFromTable.pageInfoList, 0, updateFromTable.pageInfoList.size() - 1,
					clusteringKeyValue);
			// System.out.println(location);
			p = (Page) deserialize(location);
			// recordIndex =
			// binarySearchPage(p.v,clusteringKeyValue+"",updateFromTable.clusteringKey);
			for (int i = 0; i < p.v.size(); i++) {
				Enumeration<String> e = p.v.get(i).keys();
				Object currentV = null;
				while (e.hasMoreElements()) {
					String key = e.nextElement();
					if (key.equals(updateFromTable.clusteringKey)) {
						currentV = p.v.get(i).get(key);
						break;
					}
				}

				if (clusteringKeyValue.equals(currentV)) {
					recordIndex = i;
					break;
				}
				recordIndex = -1;
			}
			// System.out.println(recordIndex+"OOOOO");

			primaryKey = clusteringKeyValue;
			break;

		case ("java.lang.double"):

			location = binarySearchDouble(updateFromTable.pageInfoList, 0, updateFromTable.pageInfoList.size() - 1,
					Double.parseDouble(clusteringKeyValue));
			p = (Page) deserialize(location);
			recordIndex = binarySearchPage(p.v, Double.parseDouble(clusteringKeyValue), updateFromTable.clusteringKey);

			primaryKey = Double.parseDouble(clusteringKeyValue);
			break;

		case ("java.util.date"):
//				String [] ymd = clusteringKeyValue.split("-",3);
//				Date date = new Date(Integer.parseInt(ymd[0]),Integer.parseInt(ymd[1]),Integer.parseInt(ymd[2]));

			DateFormat format = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
			Date date = null;
			try {
				date = format.parse(clusteringKeyValue);
				primaryKey = format.parse(clusteringKeyValue);
			} catch (ParseException e) {
				e.printStackTrace();
			}

			location = binarySearchDate(updateFromTable.pageInfoList, 0, updateFromTable.pageInfoList.size() - 1, date);
			p = (Page) deserialize(location);
			recordIndex = binarySearchPage(p.v, date, updateFromTable.clusteringKey);

			break;
		}

		if (recordIndex != -1) { // RECORD FOUND FE ONE OF THE MAIN PAGES
			Hashtable<String, Object> record = p.v.get(recordIndex);

			// Enumeration<String> e1 = record.keys();
			// Enumeration<String> e2 = columnNameValue.keys();

			record.forEach((key1, value1) -> {
				columnNameValue.forEach((key2, value2) -> {
					if (key1.equals(key2))
						record.replace(key1, value2);
				});
			});

		}

		else if (!(updateFromTable.overflowInfoList.isEmpty())) { // CHECK OVERFLOWSSSSS
			Page overF = null;

			int recordToBeUpdated = -1;
			int count = 0;
			// counter to iterate over overflowList
			while (recordToBeUpdated == -1 && count < updateFromTable.overflowInfoList.size()) {
				if (updateFromTable.overflowInfoList.get(count).pageName.startsWith(p.pageName + ".")) {

					String l = updateFromTable.overflowInfoList.get(count).location;
					overF = (Page) deserialize(l);
					recordToBeUpdated = binarySearchPage(overF.v, primaryKey, updateFromTable.clusteringKey);
					if (recordToBeUpdated != -1) {
						break;
					}
				}
				count++;

			}

			if (recordToBeUpdated == -1) {
				throw new DBAppException("RECORD NOT FOUND");
			} else {

				Hashtable<String, Object> record = overF.v.get(recordToBeUpdated);

				record.forEach((key1, value1) -> {
					columnNameValue.forEach((key2, value2) -> {
						if (key1.equals(key2)) {
							record.replace(key1, value2);
						}
					});
				});
			}
			serialize(overF);

		}

		else {
			throw new DBAppException("RECORD NOT FOUND");
		}
		serialize(p);
		serialize(updateFromTable);
		
		}
		
	}

	public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException {

		Object pk = null;
		boolean pkGiven = false;
		boolean pageFound = false;
		GridInfo gInfo = null;
	//	ArrayList<String> keyColNames = new ArrayList<>();



		Table deleteFromTable = (Table) deserialize("src/main/resources/data/" + tableName + ".ser");
		if (deleteFromTable == null) {
			throw new DBAppException("you are trying to delete from a table that doesn't exist");
		}


		String type = null;
		Enumeration<String> enumerator = columnNameValue.keys();
		
//		Enumeration<String> e2 = columnNameValue.keys();
//		while(e2.hasMoreElements()){
//			String k = e2.nextElement();
//			keyColNames.add(k); //name,age
//		}

		while (enumerator.hasMoreElements()) {
			String key = enumerator.nextElement();

			if (key.equals(deleteFromTable.clusteringKey)) {
				pkGiven = true;
				pk = columnNameValue.get(key);
				// System.out.println(pk+" DELETE");
				type = pk.getClass() + "";
				break;
			}
		}

//		Collections.sort(keyColNames);
//		for(int i = 0 ; i<deleteFromTable.grids.size();i++){
//			String []tmp = deleteFromTable.grids.get(i).colNames;
//			Arrays.sort(tmp);
//			if(Arrays.asList(tmp).equals(keyColNames)){
//				gInfo = deleteFromTable.grids.get(i);
//				break;
//			}
//		}
//jkhk;
		
		gInfo=findGrid(deleteFromTable,columnNameValue);
		
		if(gInfo!=null){//find bucket
			System.out.println("GINFO FOUNDD");
			String s = "";
			ArrayList <Integer> ranges = new ArrayList<>();
			int cellIndex;
			Vector<String> pagesToDeleteFrom = deleteFromBucket(tableName,gInfo,columnNameValue);
			if(pagesToDeleteFrom==null){
				throw new DBAppException("YOU ARE TRYING TO DELETE A RECORD THAT DOESN'T EXIST");
			}
			
			else {
				System.out.println(pagesToDeleteFrom);
				for(int h=0;h<pagesToDeleteFrom.size();h++) {
					Page delPage = (Page) deserialize(pagesToDeleteFrom.get(h));
					
					if(pkGiven) {
						System.out.println("PK GIVENNN");
						int recordToDelete = binarySearchPage(delPage.v, pk, deleteFromTable.clusteringKey);
						System.out.println(recordToDelete);
						delPage.v.removeElementAt(recordToDelete);
						System.out.println(delPage.v);
						if (delPage.v.isEmpty()) { // if page is empty after deletion but has overflows , shift overflow
							if (!(delPage.overflowLocations.isEmpty())) {

								String firstOverflowLocation = delPage.overflowLocations.get(0);

								Page firstOverflow = (Page) deserialize(firstOverflowLocation);

								PageInfo overflowInfo = null;

								for (int x = 0; x < deleteFromTable.overflowInfoList.size(); x++) {
									if (deleteFromTable.overflowInfoList.get(x).location.equals(firstOverflowLocation)) {
										overflowInfo = deleteFromTable.overflowInfoList.get(x);
									}

								}

								deleteFromTable.overflowInfoList.remove(overflowInfo);

								String locationToDelete = overflowInfo.location;

								overflowInfo.location = delPage.location;
								overflowInfo.pageName = delPage.pageName;

								delPage.overflowLocations.remove(firstOverflowLocation);

								firstOverflow.pageName = delPage.pageName;
								firstOverflow.location = delPage.location;
								firstOverflow.overflowLocations = delPage.overflowLocations;

								// find index of page i'm deleting in pageInfo
								int ind = 0;
								for (int i = 0; i < deleteFromTable.pageInfoList.size(); i++) {
									if (pagesToDeleteFrom.get(h).equals(deleteFromTable.pageInfoList.get(i).location)) {
										ind = i;
										break;
									}
								}

								deleteFromTable.pageInfoList.set(ind, overflowInfo);

								serialize(firstOverflow);

								try {

									File f = new File(locationToDelete);
									if (f.delete()) {
										System.out.println(f.getName() + " deleted");
									}

									else {
										System.out.println("failed");
									}

								} catch (Exception e) {
									e.printStackTrace();
								}

							}
						}
						
						serialize(delPage);

						
					}
					
					else { //PK NOT GIVENNNN
						
						for(int f=0;f<delPage.v.size();f++) {
							Hashtable<String, Object> currentRecord = delPage.v.get(f);
							
							Enumeration<String> e1 = columnNameValue.keys();
							Enumeration<String> e26 = currentRecord.keys();

							int k = 0;
							while (e1.hasMoreElements()) {
								String key1 = e1.nextElement();
								while (e26.hasMoreElements()) {
									String key2 = e26.nextElement();
									if (key1.equals(key2) && (currentRecord.get(key1).equals(columnNameValue.get(key2)))) {
										k++;
										break;
									}

								}

							}

							boolean canDelete = false;
							if (k == columnNameValue.size()) {
								canDelete = true;
							}

							if (canDelete) {
								delPage.v.removeElementAt(delPage.v.indexOf(currentRecord));
								if (delPage.v.isEmpty()) {
									if (!(delPage.overflowLocations.isEmpty())) {

										String firstOverflowLocation = delPage.overflowLocations.get(0);

										Page firstOverflow = (Page) deserialize(firstOverflowLocation);

										PageInfo overflowInfo = null;

										for (int x = 0; x < deleteFromTable.overflowInfoList.size(); x++) {
											if (deleteFromTable.overflowInfoList.get(x).location
													.equals(firstOverflowLocation)) {
												overflowInfo = deleteFromTable.overflowInfoList.get(x);
											}

										}

										deleteFromTable.overflowInfoList.remove(overflowInfo);

										String locationToDelete = overflowInfo.location;

										overflowInfo.location = delPage.location;
										overflowInfo.pageName = delPage.pageName;

										delPage.overflowLocations.remove(firstOverflowLocation);

										firstOverflow.pageName = delPage.pageName;
										firstOverflow.location = delPage.location;
										firstOverflow.overflowLocations = delPage.overflowLocations;

										// find index of page i'm deleting in pageInfo
										int ind = 0;
										for (int i = 0; i < deleteFromTable.pageInfoList.size(); i++) {
											if (delPage.location.equals(deleteFromTable.pageInfoList.get(i).location)) {
												ind = i;
												break;
											}
										}

										deleteFromTable.pageInfoList.set(ind, overflowInfo);

										serialize(firstOverflow);

										try {

											File f2 = new File(locationToDelete);
											if (f2.delete()) {
												System.out.println(f2.getName() + " deleted");
											}

											else {
												System.out.println("failed");
											}

										} catch (Exception e) {
											e.printStackTrace();
										}

									}

									else {
										adjustMinMax(deleteFromTable, delPage, delPage.location);
									}

								} //del page is empty
							} //end of can delete
							
							
							
							
						} //end of loop 3al page
						
						serialize(delPage);
						
					} // end of else pk not given
					
					
				} //end of loop 3al pagesToDeleteFrom
				
			}


//			Bucket b = (Bucket)deserialize(gInfo.nameToLoc.get((getBucketName(tableName,gInfo.colNames,ranges))));


		} //END OF FI GRID

		else {
		if (pkGiven) {
			String location = "";
			// CALL BINARY SEARCH
			if (type.substring(6).equalsIgnoreCase("java.lang.Integer")) {
				System.out.println("is integer");
				location = binarySearchInt(deleteFromTable.pageInfoList, 0, deleteFromTable.pageInfoList.size() - 1,
						(Integer) pk);
				// System.out.println(location);
				if (location != null) {
					pageFound = true;
				}

			} else if (type.substring(6).equalsIgnoreCase("java.lang.Double")) {
				System.out.println("is double");
				location = binarySearchDouble(deleteFromTable.pageInfoList, 0, deleteFromTable.pageInfoList.size() - 1,
						(Double) pk);

				if (location != null) {
					pageFound = true;
				}

			}

			else if (type.substring(6).equalsIgnoreCase("java.util.Date")) {
				System.out.println("is date");
				location = binarySearchDate(deleteFromTable.pageInfoList, 0, deleteFromTable.pageInfoList.size() - 1,
						(Date) pk);
				// System.out.println(location);
				if (location != null) {
					pageFound = true;
				}

			} else if (type.substring(6).equalsIgnoreCase("java.lang.String")) {
				System.out.println("is string");
				location = binarySearchString(deleteFromTable.pageInfoList, 0, deleteFromTable.pageInfoList.size() - 1,
						(String) pk);
				// System.out.println(location);
				if (location != null) {
					pageFound = true;
				}

			}

			if (pageFound == false)
				throw new DBAppException("THE RECORD YOU WANT TO DELETE DOESN'T EXIST");

			// ------------------------
			Page delPage = (Page) deserialize(location); // original page
			int recordToDelete = binarySearchPage(delPage.v, pk, deleteFromTable.clusteringKey);
			// delPage.location=location; //--------

			if (recordToDelete == -1) { // NOT FOUND IN ORIGINAL PAGE
				// LOAD OVERFLOWS

				int count = 0;
				while (recordToDelete == -1 && count < deleteFromTable.overflowInfoList.size()) {

					if (deleteFromTable.overflowInfoList.get(count).pageName.startsWith(delPage.pageName + ".")) {
						Page p170 = (Page) deserialize(deleteFromTable.overflowInfoList.get(count).location);
						recordToDelete = binarySearchPage(p170.v, pk, deleteFromTable.clusteringKey);

						if (recordToDelete != -1) {
							p170.v.remove(recordToDelete);
							serialize(p170);
							break;
						}
					}
					count++;
				}

				if (recordToDelete == -1)
					throw new DBAppException("RECORD NOT FOUND");

			} else { // I found the record in orignal page
				delPage.v.removeElementAt(recordToDelete);

				if (delPage.v.isEmpty()) { // if page is empty after deletion but has overflows , shift overflow
					if (!(delPage.overflowLocations.isEmpty())) {
//						System.out.println("OVERFLOW LOC NOT EMPTY");
//						System.out.println(delPage.location);
						// get first overflow page

						String firstOverflowLocation = delPage.overflowLocations.get(0);

						Page firstOverflow = (Page) deserialize(firstOverflowLocation);

						// firstOverflow.location=firstOverflowLocation; //------------

						PageInfo overflowInfo = null;

						for (int x = 0; x < deleteFromTable.overflowInfoList.size(); x++) {
							if (deleteFromTable.overflowInfoList.get(x).location.equals(firstOverflowLocation)) {
								overflowInfo = deleteFromTable.overflowInfoList.get(x);
							}

						}

						deleteFromTable.overflowInfoList.remove(overflowInfo);

						String locationToDelete = overflowInfo.location;

						overflowInfo.location = delPage.location;
						overflowInfo.pageName = delPage.pageName;

						delPage.overflowLocations.remove(firstOverflowLocation);

						firstOverflow.pageName = delPage.pageName;
						firstOverflow.location = delPage.location;
						firstOverflow.overflowLocations = delPage.overflowLocations;

						// find index of page i'm deleting in pageInfo
						int ind = 0;
						for (int i = 0; i < deleteFromTable.pageInfoList.size(); i++) {
							if (location.equals(deleteFromTable.pageInfoList.get(i).location)) {
								ind = i;
								break;
							}
						}

						deleteFromTable.pageInfoList.set(ind, overflowInfo);

						serialize(firstOverflow);

						try {

							File f = new File(locationToDelete);
							if (f.delete()) {
								System.out.println(f.getName() + " deleted");
							}

							else {
								System.out.println("failed");
							}

						} catch (Exception e) {
							e.printStackTrace();
						}

					}
				}

				else {
					adjustMinMax(deleteFromTable, delPage, location);
					serialize(delPage);
				}

			}
		} // end PK given

		else { // PK NOT GIVEN
			int deleted = 0;
			boolean canDelete = false;
			boolean canDeleteO = false;
			for (int jDelete = 0; jDelete < deleteFromTable.pageInfoList.size(); jDelete++) { // loop on pages of a
																								// table
				Page p180 = (Page) deserialize(deleteFromTable.pageInfoList.get(jDelete).location);
				for (int r = 0; r < p180.v.size(); r++) { // IN ONE PAGE
					Hashtable<String, Object> currentRecord = p180.v.get(r);

					Enumeration<String> e1 = columnNameValue.keys();
					Enumeration<String> e26 = currentRecord.keys();

					int k = 0;
					while (e1.hasMoreElements()) {
						String key1 = e1.nextElement();
						System.out.println(key1);
						while (e26.hasMoreElements()) {
							String key2 = e26.nextElement();
							System.out.println(key2);
							if (key1.equals(key2) && (currentRecord.get(key1).equals(columnNameValue.get(key2)))) {
								k++;
								break;
							}

						}

					}

					if (k == columnNameValue.size()) {
						canDelete = true;
					}

					if (canDelete) {
						p180.v.removeElementAt(p180.v.indexOf(currentRecord));
						deleted++;
						if (p180.v.isEmpty()) {
							if (!(p180.overflowLocations.isEmpty())) {

								String firstOverflowLocation = p180.overflowLocations.get(0);

								Page firstOverflow = (Page) deserialize(firstOverflowLocation);

								// firstOverflow.location=firstOverflowLocation; //------------

								PageInfo overflowInfo = null;

								for (int x = 0; x < deleteFromTable.overflowInfoList.size(); x++) {
									if (deleteFromTable.overflowInfoList.get(x).location
											.equals(firstOverflowLocation)) {
										overflowInfo = deleteFromTable.overflowInfoList.get(x);
									}

								}

								deleteFromTable.overflowInfoList.remove(overflowInfo);

								String locationToDelete = overflowInfo.location;

								overflowInfo.location = p180.location;
								overflowInfo.pageName = p180.pageName;

								p180.overflowLocations.remove(firstOverflowLocation);

								firstOverflow.pageName = p180.pageName;
								firstOverflow.location = p180.location;
								firstOverflow.overflowLocations = p180.overflowLocations;

								// find index of page i'm deleting in pageInfo
								int ind = 0;
								for (int i = 0; i < deleteFromTable.pageInfoList.size(); i++) {
									if (p180.location.equals(deleteFromTable.pageInfoList.get(i).location)) {
										ind = i;
										break;
									}
								}

								deleteFromTable.pageInfoList.set(ind, overflowInfo);

								serialize(firstOverflow);

								try {

									File f = new File(locationToDelete);
									if (f.delete()) {
										System.out.println(f.getName() + " deleted");
									}

									else {
										System.out.println("failed");
									}

								} catch (Exception e) {
									e.printStackTrace();
								}

							}

							else {
								adjustMinMax(deleteFromTable, p180, deleteFromTable.pageInfoList.get(jDelete).location);
							}

						}
					} // END OF CAN DELETE
					canDelete = false;
				} // end of loop counter r
				adjustMinMax(deleteFromTable, p180, deleteFromTable.pageInfoList.get(jDelete).location);
				serialize(p180);
			}

			for (int jDelete = 0; jDelete < deleteFromTable.overflowInfoList.size(); jDelete++) { // loop on pages of a
																									// table
				Page p180 = (Page) deserialize(deleteFromTable.overflowInfoList.get(jDelete).location);
				for (int r = 0; r < p180.v.size(); r++) { // IN ONE PAGE
					Hashtable<String, Object> currentRecord = p180.v.get(r);

					Enumeration<String> e1 = columnNameValue.keys();
					Enumeration<String> e26 = currentRecord.keys();

					int k = 0;
					while (e1.hasMoreElements()) {
						String key1 = e1.nextElement();
						System.out.println(key1);
						while (e26.hasMoreElements()) {
							String key2 = e26.nextElement();
							System.out.println(key2);
							if (key1.equals(key2) && (currentRecord.get(key1).equals(columnNameValue.get(key2)))) {
								k++;
								break;
							}

						}

					}

					if (k == columnNameValue.size()) {
						canDeleteO = true;
					}

					if (canDeleteO) {
						p180.v.removeElementAt(p180.v.indexOf(currentRecord));
						deleted++;

					}
					canDeleteO = false;
				} // end of loop counter r
				adjustMinMax(deleteFromTable, p180, deleteFromTable.overflowInfoList.get(jDelete).location);
				serialize(p180);
			}

			if (deleted == 0) {
				throw new DBAppException("NO RECORDS FOUND");
			}

		  } // END OF PK NOT GIVEN

		}
	} // END OF DELETE METHODDDDD

	public void insertHelper(Table t, Hashtable<String, Object> value, GridInfo gr) throws DBAppException {

		Enumeration<String> eVal = value.keys();
		Object pk = null;
		String type = null;
		while (eVal.hasMoreElements()) {
			String key = eVal.nextElement();
			if (t.clusteringKey.equals(key)) {
				pk = value.get(key);
				break;
			}
		}

		Enumeration<String> eV = t.colNameType.keys();

		while (eV.hasMoreElements()) {
			String key = eV.nextElement();
			if (t.clusteringKey.equals(key)) {
				type = t.colNameType.get(key);
				break;
			}
		}

		String location = null;
		// CALL BINARY SEARCH
		if (type.equalsIgnoreCase("java.lang.Integer")) {
			location = binarySearchInt(t.pageInfoList, 0, t.pageInfoList.size() - 1, (Integer) pk);

		} else if (type.equalsIgnoreCase("java.lang.Double")) {
			location = binarySearchDouble(t.pageInfoList, 0, t.pageInfoList.size() - 1, (Double) pk);

		} else if (type.equalsIgnoreCase("java.util.Date")) {
			// System.out.println("DATEEEE");
			location = binarySearchDate(t.pageInfoList, 0, t.pageInfoList.size() - 1, (Date) pk);

		} else if (type.equalsIgnoreCase("java.lang.String")) {
			// System.out.println((t.pageInfoList.get(0).min)+" CCCCCCC");

			// System.out.println((t.pageInfoList.get(1).min)+" BBBBBB");
			// System.out.println("BBBBBBB");

			location = binarySearchString(t.pageInfoList, 0, t.pageInfoList.size() - 1, (String) pk);
			// System.out.println("location: "+location);
		}

		// ------------DESERIALIZATION--------------
		Page p = (Page) deserialize(location);
		Object currentV = null;
		for (int y = 0; y < p.v.size(); y++) {
			Hashtable<String, Object> currentRecord = p.v.get(y);

			Enumeration<String> e = currentRecord.keys();
			while (e.hasMoreElements()) {
				String key = e.nextElement();
				if (key.equals(t.clusteringKey)) {
					currentV = currentRecord.get(key);
					break;
				}

			}

			if (currentV.equals(pk)) {
				serialize(p);
				throw new DBAppException("CAN'T INSERT.PRIMARY KEY ALREADY EXISTS");
			}

		}

		// System.out.println(p.overflowLocations.size());
		if (!(p.overflowLocations.isEmpty())) {
			// System.out.println("OVERFLOW LOCATIONS NOT EMPTY");
			for (int y = 0; y < p.overflowLocations.size(); y++) {
				Page page = (Page) deserialize(p.overflowLocations.get(y));

				for (int x = 0; x < page.v.size(); x++) {
					Hashtable<String, Object> currentRecord = page.v.get(x);

					Enumeration<String> e = currentRecord.keys();
					while (e.hasMoreElements()) {
						String key = e.nextElement();
						if (key.equals(t.clusteringKey)) {
							currentV = currentRecord.get(key);
							break;
						}

					}

					if (currentV.equals(pk)) {
						throw new DBAppException("CAN'T INSERT.PRIMARY KEY ALREADY EXISTS");
					}

				}

				serialize(page);


			}

		}

		if (p.v.size() < maxRows) { // MAFEESH 3AK FEL NEXT PAGE AKA INSERT IN CURRENT PAGE

			int insertAt = binarySearchPageInsert(p.v, pk, t.clusteringKey);
			p.v.insertElementAt(value, insertAt);

			adjustMinMax(t, p, location);
			String l =serialize(p);
			if(gr!=null){
			insertInBucket(t.tableName,l,gr,value);
			}

//				serialize(t);

		}

		else { // 3AKKKKKKK(PAGE TO INSERT IN IS FULL)
//				   case 1: last page in pageList,create new page+fi element wahed hayrooh new page
//				   case 2: not last page- 2 pages must be sorted 
			// System.out.println("PAGE TO INSERT IN IS FULL");
			// System.out.println(t.pageInfoList.get(t.pageInfoList.size()-1).location);
			if (location.equals((t.pageInfoList.get(t.pageInfoList.size() - 1)).location)) { // last page
				// System.out.println("LAST PAGEEEE");
				Page lastPage = (Page) deserialize(location);
				Page newPage = new Page();
				newPage.pageName = t.tableName + (t.pageInfoList.size() + 1);

				int insertAt = binarySearchPageInsert(lastPage.v, pk, t.clusteringKey);
				lastPage.v.insertElementAt(value, insertAt); // OR SORT

				Hashtable<String, Object> lastRecord = lastPage.v.lastElement();

				newPage.v.add(lastRecord);
				lastPage.v.remove(lastRecord);

				String loc = serialize(newPage);

				newPage.location = loc;

				PageInfo pInfo = new PageInfo();

				pInfo.pageName = newPage.pageName;

				pInfo.location = loc;
				String clusteringKey = t.clusteringKey;
				lastRecord.forEach((key, value1) -> {
					if (clusteringKey.equals(key))
						pInfo.min = value1;
					pInfo.max = value1;
				});

				t.pageInfoList.add(pInfo);

				adjustMinMax(t, lastPage, location);
				serialize(lastPage);

				if(gr!=null){
					if(lastRecord.equals(value)){
						insertInBucket(t.tableName,newPage.location,gr,lastRecord);
					}
					else{
						//update pointer
						deleteFromBucket(t.tableName,gr,lastRecord);
						insertInBucket(t.tableName,newPage.location,gr,lastRecord);
						insertInBucket(t.tableName,lastPage.location,gr,value);
					}


				}

			}

			else { // TRYING TO INSERT IN A FULL PAGE+IT'S NOT THE LAST PAGE
					// System.out.println("INSERTING IN FULL PAGE+NOT LAST");
					// Page currentPage=(Page)deserialize(location);
				Page currentPage = p;
				int nextIndex = 0;

				for (int i = 0; i < t.pageInfoList.size(); i++) {
					if (location.equals(t.pageInfoList.get(i).location)) {
						nextIndex = i + 1;
						break;
					}

				}


				String nextPageLocation = t.pageInfoList.get(nextIndex).location;

				// System.out.println("NEXT PAGE LOC: "+nextPageLocation);

				Page nextPage = (Page) deserialize(nextPageLocation);

				if ((nextPage.v.size()) == maxRows) { // IF NEXT PAGE IS FULL CREATE OVERFLOW

					if (currentPage.overflowLocations.isEmpty()) { // CREATING FIRST OVERFLOW

						Page firstOverflow = new Page();

						firstOverflow.pageName = currentPage.pageName + ".1";

						firstOverflow.v.insertElementAt(value, 0);

						String loc = serialize(firstOverflow);

						PageInfo pInfo = new PageInfo();

						pInfo.pageName = firstOverflow.pageName;

						firstOverflow.location = loc;
						pInfo.location = loc;

						String clusteringKey = t.clusteringKey;
						value.forEach((key, value1) -> {
							if (clusteringKey.equals(key)) {
								pInfo.min = value1;
								pInfo.max = value1;
							}

						});

						t.overflowInfoList.add(pInfo);

						currentPage.overflowLocations.add(loc);

						serialize(currentPage);
						if(gr!=null){
							insertInBucket(t.tableName,loc,gr,value);
						}
						return;



					}


					String lastOverflowLocation = currentPage.overflowLocations
							.get(currentPage.overflowLocations.size() - 1);
					Page lastOverflow = (Page) deserialize(lastOverflowLocation);
					if (pageFull(lastOverflow)) {
						Page newOverflow = new Page();
						newOverflow.pageName = currentPage.pageName + "." + (currentPage.overflowLocations.size() + 1);

						newOverflow.v.insertElementAt(value, 0);

						String loc = serialize(newOverflow);

						PageInfo pInfo = new PageInfo();
						pInfo.location = loc;
						String clusteringKey = t.clusteringKey;
						value.forEach((key, value1) -> {
							if (clusteringKey.equals(key))
								pInfo.min = value1;
							pInfo.max = value1;
						});

						t.overflowInfoList.add(pInfo);
						currentPage.overflowLocations.add(loc);

						serialize(currentPage);

						if(gr!=null){
							insertInBucket(t.tableName,loc,gr,value);
						}

					}

					else { // LAST OVERFLOW is not full
						if (lastOverflow.v.size() == 0) {
							lastOverflow.v.insertElementAt(value, 0);

						} else {
							int insertAt = binarySearchPageInsert(lastOverflow.v, pk, t.clusteringKey);
							lastOverflow.v.insertElementAt(value, insertAt);
						}

						adjustMinMax(t, lastOverflow, lastOverflowLocation);
						String loc = serialize(lastOverflow);
						if(gr!=null){
							insertInBucket(t.tableName,loc,gr,value);
						}

					}

				}

				else { // NEXT PAGE NOT FULL THEREFORE INSERT IN CURRENT THEN SHIFT LAST RECORD TO NEXT
						// System.out.println("NEXT PAGE NOT FULL");

					int insertAt = binarySearchPageInsert(currentPage.v, pk, t.clusteringKey);

					// System.out.println("INSERT AT "+insertAt);
					currentPage.v.insertElementAt(value, insertAt); // OR SORT
					Hashtable<String, Object> lastRecord = currentPage.v.lastElement();
					nextPage.v.insertElementAt(lastRecord, 0);
					currentPage.v.remove(lastRecord);

					// System.out.println(currentPage.v);
					// System.out.println(nextPage.v);

					adjustMinMax(t, currentPage, location);
					serialize(currentPage);

					adjustMinMax(t, nextPage, nextPageLocation);
					serialize(nextPage);

					if(gr!=null){
						if(lastRecord.equals(value)){
							insertInBucket(t.tableName,nextPage.location,gr,lastRecord);
						}
						else{
							//update pointer
							deleteFromBucket(t.tableName,gr,lastRecord);
							insertInBucket(t.tableName,nextPage.location,gr,lastRecord);
							insertInBucket(t.tableName,currentPage.location,gr,value);
						}
					}

				}

			}

		} // END OF ELSE PAGE I'M TRYING TO INSERT IN IS FULL

	} // END OF INSERT HELPER

	public static int binarySearchPageInsert(Vector<Hashtable<String, Object>> v, Object target, String clusteringKey) {
		Object fValue = null, lValue = null;
		Object primaryKey;
		int compareWith = 0;

		Enumeration<String> e = v.get(0).keys();
		while (e.hasMoreElements()) {
			String key = e.nextElement();
			if (key.equals(clusteringKey)) {
				fValue = (v.get(0)).get(key);
				break;
			}

		}
		Enumeration<String> e2 = v.get(v.size() - 1).keys();
		while (e2.hasMoreElements()) {
			String key = e2.nextElement();
			if (key.equals(clusteringKey)) {
				lValue = (v.get(v.size() - 1)).get(key);
				break;
			}

		}

		if (target instanceof Integer) {
			primaryKey = (Integer) target;
			compareWith = 0;

			if (((Integer) primaryKey).intValue() <= ((Integer) fValue).intValue()) {
				return 0;
			}
			if (((Integer) primaryKey).intValue() >= ((Integer) lValue).intValue()) {
				return (v.size());

			}
		}

		else if (target instanceof Double) {
			primaryKey = (Double) target;
			compareWith = 1;

			if ((Double) primaryKey <= (Double) fValue)
				return 0;
			if ((Double) primaryKey >= (Double) lValue)
				return (v.size());

		} else if (target instanceof Date) {
			primaryKey = (Date) target;
			compareWith = 2;
			if (((Date) primaryKey).compareTo((Date) fValue) <= 0)
				return 0;
			if (((Date) primaryKey).compareTo((Date) lValue) >= 0)
				return (v.size());
		}

		else if (target instanceof String) {
			primaryKey = (String) target;
			compareWith = 3;

			if (((String) primaryKey).compareTo((String) fValue) <= 0)
				return 0;
			if (((String) primaryKey).compareTo((String) lValue) >= 0)
				return (v.size());
		}

		int first = 0;
		int last = v.size() - 1;
		int mid = 0;
		Object midV = null;
		Object prevV = null;
		Object nextV = null;

		while ((first <= last) && (mid < v.size() - 2)) {
			mid = (first + last) / 2;

			Enumeration<String> e4 = v.get(mid).keys();
			while (e4.hasMoreElements()) {
				String key = e4.nextElement();
				if (key.equals(clusteringKey)) {
					midV = v.get(mid).get(key);
					break;
				}
			}

			if (mid > 0) {
				Enumeration<String> e5 = v.get(mid - 1).keys();
				while (e5.hasMoreElements()) {
					String key = e5.nextElement();
					if (key.equals(clusteringKey)) {
						prevV = v.get(mid - 1).get(key);
						break;
					}
				}
			}

			Enumeration<String> e6 = v.get(mid + 1).keys();
			while (e6.hasMoreElements()) {
				String key = e6.nextElement();
				if (key.equals(clusteringKey)) {
					nextV = v.get(mid + 1).get(key);
					break;
				}
			}

			switch (compareWith) {
			case 0:
				if (((Integer) target).intValue() < ((Integer) midV).intValue()) {
					if (mid > 0 && ((Integer) target).intValue() > ((Integer) prevV).intValue()) {
						return mid; // 1 15
					}

					last = mid - 1;
				} else if (((Integer) target).intValue() > ((Integer) midV).intValue()) {
					if (mid > 0 && ((Integer) target).intValue() < ((Integer) nextV).intValue()) {
						return mid + 1;
					}
					first = mid + 1;
				}

				else {
					return mid;
				}

				break;

			case 1:
				if ((Double) target < (Double) midV) {
					if (mid > 0 && ((Double) target) > ((Double) prevV)) {
						return mid;
					}

					last = mid - 1;
				} else if ((Double) target > (Double) midV) {
					if (mid > 0 && ((Double) target) < ((Double) nextV)) {
						return mid + 1;
					}
					first = mid + 1;
				}

				else {
					return mid;
				}

				break;

			case 2:

				if (((Date) target).compareTo((Date) midV) < 0) {
					if (mid > 0 && ((Date) target).compareTo((Date) prevV) > 0) {
						return mid;

					}

					last = mid - 1;
				} else if (((Date) target).compareTo((Date) midV) > 0) {
					if (mid > 0 && (((Date) target).compareTo((Date) nextV) < 0)) {
						return mid + 1;
					}
					first = mid + 1;
				}

				else {
					return mid;
				}

				break;

			case 3:

				if (((String) target).compareTo((String) midV) < 0) {
					if (mid > 0 && ((String) target).compareTo((String) prevV) > 0)
						return mid;

					last = mid - 1;
				} else if (((String) target).compareTo((String) midV) > 0) {
					if (mid > 0 && ((String) target).compareTo((String) nextV) < 0) {
						return mid + 1;
					}

					first = mid + 1;
				}

				else {
					return mid;
				}

				break;
			}

		}
		return mid;

	}

	public int binarySearchPage(Vector<Hashtable<String, Object>> v, Object key, String clusteringKey) {
		// PAGE LOADED
		Object pk = null;
		Object currentValue = null;
		int first = 0;
		int last = v.size() - 1;
		int mid = (first + last) / 2; // index of page el fe nos el arraylist
		int compareWith = -1;
		if (key instanceof Integer) {
			pk = (Integer) key;
			// System.out.println("Integer");

			compareWith = 0;
		}

		else if (key instanceof Double) {
			pk = (Double) key;
			// System.out.println("Double");

			compareWith = 1;
		} else if (key instanceof Date) {
			pk = (Date) key;
			// System.out.println("Date");
			// System.out.println(pk+"---------");

			compareWith = 2;
		}

		else if (key instanceof String) {
			pk = (String) key;
			compareWith = 3;
			// System.out.println("FFFFFF");
			// System.out.println(pk);

		}

//		boolean found=false;
		while (first <= last) {

			Enumeration<String> s = v.get(mid).keys();

			while (s.hasMoreElements()) {
				String k = s.nextElement();
				if (k.equals(clusteringKey)) {
					// System.out.println(k);
					currentValue = v.get(mid).get(k);
					// System.out.println(currentValue);
					break;
				}
			}
			// System.out.println(currentValue+"NNNNNNN");
			// System.out.println("pk: "+pk);
			// System.out.println("current:"+currentValue);

			if (compareWith == 0) {

				if ((int) pk > (int) currentValue) {
					first = mid + 1;
				} else if (pk.equals(currentValue)) {
					// System.out.println(currentValue+"ZZZZ");
					return mid;
					// found = true; break;

				} else {
					last = mid - 1;
				}

				mid = (first + last) / 2;
			} // END OF COMPARE WITH=0

			else if (compareWith == 1) {

				if (Double.compare((double) pk, (Double) currentValue) > 0) {
					first = mid + 1; // discard the first half
				} else if ((Double.compare((double) pk, (Double) currentValue) == 0)) {
					return mid;
					// found = true; break;
				} else if (Double.compare((double) pk, (Double) currentValue) < 0) {
					last = mid - 1; // discard the last half
				}

				mid = (first + last) / 2;
			}

			else if (compareWith == 2) {

				if (((Date) pk).compareTo((Date) currentValue) > 0) {
					first = mid + 1;
				} else if (((Date) pk).compareTo((Date) currentValue) == 0) {
					return mid;
					// found = true; break;
				} else if (((Date) pk).compareTo((Date) currentValue) < 0) {
					last = mid - 1;
				}

				mid = (first + last) / 2;

			} else if (compareWith == 3) {
				if ((pk + "").compareTo(currentValue + "") > 0) {
					first = mid + 1;
				} else if ((pk + "").equals(currentValue + "")) {
					// System.out.println("FOUNDDDDD");
					return mid;
					// found = true; break;
				} else if ((pk + "").compareTo(currentValue + "") < 0) {
					// System.out.println("NOS EL TANY");
					last = mid - 1;
				}
				mid = (first + last) / 2;

			}

		}
		return -1;

	}

	public String binarySearchInt(ArrayList<PageInfo> pages, int first, int last, Integer key) {

		int mid = (first + last) / 2; // index of page el fe nos el arraylist

		Object mx = pages.get(pages.size() - 1).max;
		Object mn = pages.get(0).min;
		if (key >= (Integer) mx) {
			return pages.get(pages.size() - 1).location;
		}

		if (key <= (Integer) mn) {
			return pages.get(0).location;
		}

		// System.out.println(key+"...........");

		while (first <= last) {
			Object max = (Integer) pages.get(mid).max;
			Object min = (Integer) pages.get(mid).min;
			// System.out.println(max);
			// System.out.println(min);

			if (key.intValue() > ((Integer) max).intValue()) {
				if ((Integer) pages.get(mid + 1).min > key) {
					return pages.get(mid).location;
				} else {
					first = mid + 1;
				}
			}

			else if ((key.intValue() > ((Integer) min).intValue()) && (key.intValue() < ((Integer) max).intValue())) {
				// ((key> (Integer)min) && (key<(Integer)max)){
				// System.out.println("Page is found at index: " + mid);
				return pages.get(mid).location;
			} else if (key.intValue() < ((Integer) min).intValue()) {
				last = mid - 1;
			}
			mid = (first + last) / 2;
		}
		return null;
	}

	public String binarySearchDouble(ArrayList<PageInfo> pages, int first, int last, Double key) {

		int mid = (first + last) / 2; // index of page el fe nos el arraylist

		if (key >= (Double) pages.get(pages.size() - 1).max) {
			return pages.get(pages.size() - 1).location;
		}

		if (key <= (Double) pages.get(0).min) {
			return pages.get(0).location;
		}

		while (first <= last) {
			Object max = pages.get(mid).max;
			Object min = pages.get(mid).min;
			if (Double.compare(key, (Double) max) > 0) {
				if (key.compare(key, (Double) (pages.get(mid + 1).min)) < 0) {
					return pages.get(mid).location;
				} else {
					first = mid + 1;
				}

			} else if ((Double.compare(key, (Double) min) > 0) && (Double.compare(key, (Double) max) < 0)) {
				// System.out.println("Page is found at index: " + mid);
				return pages.get(mid).location;
			} else if (Double.compare(key, (Double) min) < 0) {
				last = mid - 1; // discard the last half
			}
			mid = (first + last) / 2;
		}
		return null;
	}

	public String binarySearchDate(ArrayList<PageInfo> pages, int first, int last, Date key) {

		if (key.compareTo((Date) pages.get(pages.size() - 1).max) >= 0) {
			return pages.get(pages.size() - 1).location;
		}

		if (key.compareTo((Date) pages.get(0).min) <= 0) {
			return pages.get(0).location;
		}

		int mid = (first + last) / 2; // index of page el fe nos el arraylist
		while (first <= last) {
			// System.out.println((key.compareTo((Date)pages.get(mid).max)<0 )+"JJJJJ");
			// System.out.println((key.compareTo((Date)pages.get(mid).min)>0 )+"JJJJJ");

			if (key.compareTo((Date) pages.get(mid).max) > 0) {
				if (key.compareTo((Date) pages.get(mid + 1).min) < 0) {
					return pages.get(mid).location;
				} else {
					first = mid + 1;
				}

			} else if ((key.compareTo((Date) pages.get(mid).min) > 0)
					&& (key.compareTo((Date) pages.get(mid).max) < 0)) {
				// System.out.println("Page is found at index: " + mid);
				return pages.get(mid).location;
				// break;
			} else if (key.compareTo((Date) pages.get(mid).min) < 0) {
				last = mid - 1;
			}
			mid = (first + last) / 2;
		}
		return null;
	}

	public String binarySearchString(ArrayList<PageInfo> pages, int first, int last, String key) {

		// System.out.println((String)(pages.get(pages.size()-1).max));
		// System.out.println("aaaa");
		// System.out.println(key);
		// System.out.println(pages.get(pages.size()-1).max);
		// System.out.println(pages.get(0).min);

		// String x= pages.get(0).min+"";

		// System.out.println(x);
		// System.out.println(key.compareTo((String)pages.get(0).min));
		// System.out.println("XXXXXX");

		if ((key.compareTo((pages.get(pages.size() - 1).max) + "") >= 0)) {
			return pages.get(pages.size() - 1).location;
		}

		if (key.compareTo(pages.get(0).min + "") <= 0) {
			// System.out.println("LESS THAN MIN");

			return pages.get(0).location;
		}

		int mid = (first + last) / 2; // index of page el fe nos el arraylist
		while (first <= last) {
			if (key.compareTo(pages.get(mid).max + "") > 0) {
				if (key.compareTo(pages.get(mid + 1).min + "") < 0) {
					return pages.get(mid).location;
				} else {
					first = mid + 1;
				}
			} else if ((key.compareTo(pages.get(mid).min + "") >= 0) && (key.compareTo(pages.get(mid).max + "") <= 0)) {
				// System.out.println("Page is found at index: " + mid);
				return pages.get(mid).location;
				// return pages.indexOf(pages.get(mid));
				// break;
			} else if (key.compareTo(pages.get(mid).min + "") < 0) {

				last = mid - 1;
			}
			mid = (first + last) / 2;
		}

		return null;
	}

	public static String serialize(Object o) {

		String location = "";

		if (o instanceof Table) {
			location = "src/main/resources/data/" + ((Table) o).tableName + ".ser";
		}

		if (o instanceof Page) {
			location = "src/main/resources/data/" + ((Page) o).pageName + ".ser";
		}

//		if (o instanceof Grid) {
//			location = "src/main/resources/data/" + ((Grid) o).gridName + ".ser";
//		}
		
		if (o instanceof Bucket) {
			location = "src/main/resources/data/" + ((Bucket) o).bucketName + ".ser";
		}
		
		
		// Serialization
		try {
			FileOutputStream file = new FileOutputStream(location);
			ObjectOutputStream out = new ObjectOutputStream(file);

			out.writeObject(o); // serialize el vector kaman?
			out.close();
			file.close();
			// System.out.println("SERIALIZATION COMPLETE");

		} catch (IOException ex) {
			System.out.println("IOException is caught");
		}

		return location;
	}

	@SuppressWarnings("resource")
	public static Object deserialize(String location) {
		// Deserialization
		Object o = null;
		try {
			FileInputStream file = new FileInputStream(location);
			ObjectInputStream in = new ObjectInputStream(file);
			o = in.readObject();
			if (o instanceof Page) {
				// o = (Page)in.readObject();
				return (Page) o;

			}

			if (o instanceof Table) {
				// o= (Table)in.readObject();
				return (Table) o;
			}

//    		  ObjectInputStream in=new ObjectInputStream(new FileInputStream(location));  
//    		  Table  t=(Table)in.readObject();  

			in.close();

			// file.close();
			// System.out.println("Object has been deserialized ");

		}

		catch (IOException ex) {
			System.out.println("IOException is caught");
		}

		catch (ClassNotFoundException ex) {
			System.out.println("ClassNotFoundException is caught");
		}

		return o;

	}

	public void writeToCSV(Table t) {

		try {
			writer = new FileWriter("src/main/resources/metadata.csv", true);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		t.colNameType.forEach((key, value) -> {
			try {
				writer.append(t.tableName + ",");
				writer.append(key + "," + value + ",");
				if (key.equals(t.clusteringKey)) {
					writer.append("True");
				} else {
					writer.append("False");

				}
				// INDEXED??????????

                writer.append(",False");
				writer.append("," + t.colNameMin.get(key));
				writer.append("," + t.colNameMax.get(key));
				writer.append("\n");
				

			} catch (IOException e) {

				e.printStackTrace();
			}
		});

		try {
			writer.flush();
			writer.close();

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public boolean pageFull(Page p) {
		if (p.v.size() >= maxRows) { // REMOVE >
			return true;
		}

		else {
			return false;

		}

	}

	public void adjustMinMax(Table t, Page p, String location) {

		for (int i = 0; i < t.pageInfoList.size(); i++) {

			if (t.pageInfoList.get(i).location.equals(location)) {

				if (p.v.size() == 0) {
					t.pageInfoList.get(i).min = null;
					t.pageInfoList.get(i).max = null;
					return;

				}

				Enumeration<String> e = p.v.get(0).keys();
				while (e.hasMoreElements()) {
					String key = e.nextElement();

					if (key.equals(t.clusteringKey)) {
						t.pageInfoList.get(i).min = p.v.get(0).get(key);
						break;
					}
				}

				Enumeration<String> e2 = p.v.get(p.v.size() - 1).keys();
				while (e2.hasMoreElements()) {
					String key = e2.nextElement();

					if (key.equals(t.clusteringKey)) {
						t.pageInfoList.get(i).max = p.v.get(p.v.size() - 1).get(key);
						break;
					}
				}
				return;
			}

		}

	}



	@SuppressWarnings("rawtypes")
	public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {
//	        arrSQLTerms[0]._strTableName = "Student";
//			arrSQLTerms[0]._strColumnName= "name";
//			arrSQLTerms[0]._strOperator = "=";
//			arrSQLTerms[0]._objValue = "John Noor";

//			arrSQLTerms[1]._strTableName = "Student";
//			arrSQLTerms[1]._strColumnName= "gpa";
//			arrSQLTerms[1]._strOperator = "=";
//			arrSQLTerms[1]._objValue= new Double( 1.5 );

		int cmp=-1;
		Table selectTable = null;
		String t = "";
		boolean pageFound = false;
		Vector<Vector<Hashtable<String, Object>>> result = new Vector<Vector<Hashtable<String, Object>>>();
		Vector<Vector<String>> buckets = new Vector<Vector<String>>();
		
		boolean tableFound=false;
		
			BufferedReader br = null;
			try {
				br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));

			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}

			String current = null;
			try {

				current = br.readLine();
				//current = br.readLine();

			} catch (IOException e) {
				e.printStackTrace();
			}

			while (current != null) { // CHECK INDEXED
				String[] line = current.split(",");
				String tName = line[0];
				String colName = line[1];
				String colType = line[2];

				@SuppressWarnings("unused")
				String clusteringKey = line[3];
				
				if(tName.equals(sqlTerms[0]._strTableName)) {
					tableFound=true;
				}
				

				try {
					current = br.readLine();
				} catch (IOException e) {

					e.printStackTrace();
				}
			}

			try {
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			
			if(!tableFound) {
				throw new DBAppException("TABLE DOES NOT EXIST.");	
			}
			
			else {
				selectTable = (Table) deserialize("src/main/resources/data/" + sqlTerms[0]._strTableName + ".ser");

			}
			
			ArrayList<String> searchColumns = new ArrayList<String>();
			Hashtable<String,Object> sqlHash = new Hashtable<String,Object>();
			for(int d=0;d<sqlTerms.length;d++) {
				searchColumns.add(sqlTerms[d]._strColumnName);
				sqlHash.put(sqlTerms[d]._strColumnName, sqlTerms[d]._objValue);
				
			}
			

//			Collections.sort(searchColumns);
//			GridInfo gInfo = null;
//			boolean isPartial = true ;
//			int max = 0 ;
//			int cur = 0 ;
//			for(int a = 0 ; a<selectTable.grids.size();a++){
//				String []tmp = selectTable.grids.get(a).colNames;
//				Arrays.sort(tmp);
//				if(Arrays.asList(tmp).equals(searchColumns)){
//					gInfo = selectTable.grids.get(a);
//					isPartial = false;
//					break;
//				}
//				else {
//
//					for(int b = 0 ;b< tmp.length;b++){
//					  for(int o = 0 ; o<searchColumns.size();o++ ){
//						if(tmp[b].equals(searchColumns.get(o))){
//							cur++;
//							break;
//						}
//
//					}
//				}
//					if(cur>max){
//						max = cur;
//						gInfo = selectTable.grids.get(a);
//					}
//					cur = 0;
//				}
//
//			}
			
			GridInfo gInfo=	findGrid(selectTable,sqlHash);

			
			if(gInfo!=null){//find bucket
				String s = "";
				//ArrayList <Integer> ranges = new ArrayList<>();
				int cellIndex = 0;

					
					for(int j=0;j<sqlTerms.length;j++) {
						
						//john,20 --page2
						
						Object val= sqlHash.get(searchColumns.get(j));
						
						String st="";
//						if (val instanceof String && (val+"").charAt(0)>=49  
//								&& (val+"").charAt(0)<=57 ) { //id for example
//									
//							    String[] temp = (val+"").split("-");
//				                st = temp[0]+temp[1];
//								cellIndex = Arrays.binarySearch(gInfo.gridRanges.get(sqlTerms[j]._strColumnName), 0, gInfo.gridRanges.get(sqlTerms[j]._strColumnName).length, st);
//						}
//						else {
//							cellIndex = Arrays.binarySearch(gInfo.gridRanges.get(sqlTerms[j]._strColumnName), 0, gInfo.gridRanges.get(sqlTerms[j]._strColumnName).length, val);
//						}
					
					if(Arrays.asList(gInfo.colNames).contains(sqlTerms[j]._strColumnName))	{
						if (val instanceof String) { //id for example

							if((val+"").charAt(0)>=49  
									&& (val+"").charAt(0)<=57) {
								 	String[] temp = (val+"").split("-");
								 	
					                st = temp[0]+temp[1];
					                
									if(gInfo.gridRanges.get(sqlTerms[j]._strColumnName)[gInfo.gridRanges.get(sqlTerms[j]._strColumnName).length-1]==null) {
										cellIndex = Arrays.binarySearch(gInfo.gridRanges.get(sqlTerms[j]._strColumnName), 0, gInfo.gridRanges.get(sqlTerms[j]._strColumnName).length-1, Integer.parseInt(st));

									}
									else {
										cellIndex = Arrays.binarySearch(gInfo.gridRanges.get(sqlTerms[j]._strColumnName), 0, gInfo.gridRanges.get(sqlTerms[j]._strColumnName).length, Integer.parseInt(st));
										
									}
					                
									
							}
							
							else {
								
								if(gInfo.gridRanges.get(sqlTerms[j]._strColumnName)[gInfo.gridRanges.get(sqlTerms[j]._strColumnName).length-1]==null) {
									cellIndex = Arrays.binarySearch(gInfo.gridRanges.get(sqlTerms[j]._strColumnName), 0, gInfo.gridRanges.get(sqlTerms[j]._strColumnName).length-1, ((String)val).charAt(0));

								}
								else {
									cellIndex = Arrays.binarySearch(gInfo.gridRanges.get(sqlTerms[j]._strColumnName), 0, gInfo.gridRanges.get(sqlTerms[j]._strColumnName).length, ((String)val).charAt(0));
									
								}
								
							}
						
						}
						
						else if(val instanceof Date) {
							String d=val+"";
							
							String str=getDate(d);
							
							DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

							LocalDate localDate = LocalDate.parse(str, formatter);
							
							
							if(gInfo.gridRanges.get(sqlTerms[j]._strColumnName)[gInfo.gridRanges.get(sqlTerms[j]._strColumnName).length-1]==null) {
								cellIndex = Arrays.binarySearch(gInfo.gridRanges.get(sqlTerms[j]._strColumnName), 0, gInfo.gridRanges.get(sqlTerms[j]._strColumnName).length-1, localDate);

							}
							else {
								cellIndex = Arrays.binarySearch(gInfo.gridRanges.get(sqlTerms[j]._strColumnName), 0, gInfo.gridRanges.get(sqlTerms[j]._strColumnName).length, localDate);
								
							}
							

						}
						
						else {
							
							if(gInfo.gridRanges.get(sqlTerms[j]._strColumnName)[gInfo.gridRanges.get(sqlTerms[j]._strColumnName).length-1]==null) {
								cellIndex = Arrays.binarySearch(gInfo.gridRanges.get(sqlTerms[j]._strColumnName), 0, gInfo.gridRanges.get(sqlTerms[j]._strColumnName).length-1, val);

							}
							else {
								cellIndex = Arrays.binarySearch(gInfo.gridRanges.get(sqlTerms[j]._strColumnName), 0, gInfo.gridRanges.get(sqlTerms[j]._strColumnName).length, val);
								
							}

							//System.out.println(cellIndex);
							//System.out.println("EVERYTHING IS FINEEEE");
						}
						
						
						
						if (cellIndex < 0) {
							cellIndex = (cellIndex * (-1)) - 2;
						}
						
					
						Vector<String> bucketLocations = new Vector<>();
						Enumeration<String> e = gInfo.nameToLoc.keys();
						//loop on all buckets
						while (e.hasMoreElements()){
							String key = e.nextElement();
							//split bucket name on #
							String [] bucketContent = key.split("#");
							
							switch(sqlTerms[j]._strOperator) {
							
								case "=" : if(Arrays.asList(bucketContent).contains(searchColumns.get(j)+cellIndex)){
											 bucketLocations.add(gInfo.nameToLoc.get(key));
										   } break;
										
								case "!=" : bucketLocations.add(gInfo.nameToLoc.get(key)); break;
								
								case "<"  : 	
												
								case "<=" : for(int v=0;v<=cellIndex;v++) {
											 if(Arrays.asList(bucketContent).contains(searchColumns.get(j)+v)){
										       bucketLocations.add(gInfo.nameToLoc.get(key));
							   			     }
									
									        }
								
									       break;
									
								case ">" : 
									
								case ">=" : for(int v=cellIndex;v<10;v++) {
							 		 		  if(Arrays.asList(bucketContent).contains(searchColumns.get(j)+v)){
								               bucketLocations.add(gInfo.nameToLoc.get(key));
					   			              }
							
							               }
						
							               break;
								
							
							}
							
							

						} //end of while enumeration
						
						buckets.add(bucketLocations);
						
						bucketLocations.clear();
						
						//tableName#colName0,value0#colName1,value1#
					  }	
					} //END OF SQL TERMS
					
					//String bucketName= getBucketName(selectTable.tableName,gInfo.colNames,ranges);
					//String bucketLoc = gInfo.nameToLoc.get(bucketName);
					//Bucket b = (Bucket) deserialize(bucketLoc);
					//pageLocations = b.bucketValues.get(s);
					//V1 b1,b2,b3,b4 all name=Amira
					//V2 b1,b2,b3,b5 all age>20
					//V3 b4,b5 salary=2000
					
					Vector<String> currentBuckets = new Vector<String>();

					for (int r = 0; r < buckets.size(); r++) {
						String operator = arrayOperators[r];

						if (currentBuckets.size() == 0) {
							currentBuckets.addAll(buckets.get(r));

						}

						if (currentBuckets.size() == 0 && r<currentBuckets.size()-1 ) {
							//System.out.println("CURRENT EMPTY");
							currentBuckets.addAll(buckets.get(r));

						}

						switch (operator) {
							case "AND":
							 if(r<buckets.size()-1) {
								for (int s1 = 0; s1 < currentBuckets.size(); s1++) { // LOOP ON RECORDS
								 if (buckets.get(r + 1).contains(currentBuckets.get(s1))) {
									continue;
								} else {
									currentBuckets.remove(currentBuckets.get(s1));
								}

							  }
								
							} ;
							
							break;

						case "OR":
							if(r<buckets.size()-1) {
			    				for (int s1 = 0; s1 < buckets.get(r + 1).size(); s1++) { // LOOP ON RECORDS
			    					if (!currentBuckets.contains(buckets.get(r + 1).get(s1)))
			    						currentBuckets.add(buckets.get(r + 1).get(s1));
			    				}

								
							} ;

							
							break;

						case "XOR":

							if(r<buckets.size()-1) {

							 for (int c = 0; c < buckets.get(r + 1).size(); c++) { // LOOP ON RECORDS
								if (!currentBuckets.contains(buckets.get(r + 1).get(c)))
									currentBuckets.add(buckets.get(r + 1).get(c));


								else {
									currentBuckets.remove(buckets.get(r + 1).get(c)); // SIZE CHANGES???

								}
								
							  } //END OF FOR LOOP
							
							} ;
							
							break;

						}

					} //END OF LOOP 3AL BUCKETS
					
//					amira,20
//					String indexedVal="";
//					for(int a = 0 ; a<gInfo.colNames.length;a++){
//						Enumeration<String> e3 = sqlHash.keys();
//						while (e3.hasMoreElements()){
//							String key = e3.nextElement();
//							if(key.equals(gInfo.colNames[a])) {
//								indexedVal+= sqlHash.get(key)+"";
//								if(a!=gInfo.colNames.length-1) {
//									indexedVal+=",";
//								}
//								
//							}
//
//						}
//
//						
//					} //john noor,1.5
					
					Vector<String> pageLocations=new Vector<String>();
					for (int p=0;p<currentBuckets.size();p++) {
						Bucket b= (Bucket) deserialize(currentBuckets.get(p));
						
						Enumeration<String> e2 = b.bucketValues.keys();
						//loop on all buckets
						while (e2.hasMoreElements()){
							String key = e2.nextElement();
							for(int k=0;k<b.bucketValues.get(key).size();k++) {
								pageLocations.add(b.bucketValues.get(key).get(k));
							}
							//pageLocations.add(b.bucketValues.get(key));
							
						}
						
						for (int q=0;q<b.overflowBucketLocations.size();q++) {
//							Vector<String> v=new Vector<String>();
//							v.add(b.overflowBucketLocations.get(q));
							pageLocations.add(b.overflowBucketLocations.get(q));
						}
						

					}
					
					for(int n=0;n<pageLocations.size();n++) {
						
						Page p180 = (Page) deserialize(pageLocations.get(n));
						for (int r = 0; r < p180.v.size(); r++) { // IN ONE PAGE
							Hashtable<String, Object> currentRecord = p180.v.get(r);

							Enumeration<String> e130 = currentRecord.keys();
							while (e130.hasMoreElements()) {
								String key = e130.nextElement();
								
								//--------------------------------------------
								
							 for(int i=0;i<sqlTerms.length;i++) {	
								switch (sqlTerms[i]._strOperator) {
									case "=" : 
										if (key.equals(sqlTerms[i]._strColumnName) && currentRecord.get(key).equals(sqlTerms[i]._objValue)) {
											result.get(i).add(currentRecord);
										} ;break;
										
									case "!=" :
										if (key.equals(sqlTerms[i]._strColumnName) && !(currentRecord.get(key).equals(sqlTerms[i]._objValue))) {
											result.get(i).add(currentRecord);
										} ;break;
										
									case ">" :
										
										if (sqlTerms[i]._objValue instanceof Integer) {
											if (key.equals(sqlTerms[i]._strColumnName) && (((Integer)currentRecord.get(key)).intValue()>(Integer)sqlTerms[i]._objValue)) {
												result.get(i).add(currentRecord);
										    }
									    }
										
										else if (sqlTerms[i]._objValue instanceof Double) {
											if (key.equals(sqlTerms[i]._strColumnName) && (((Double)currentRecord.get(key)).doubleValue()>(Double)sqlTerms[i]._objValue)) {
												result.get(i).add(currentRecord);
										    }
									    }
										
										else {
										  if (key.equals(sqlTerms[i]._strColumnName) && ((currentRecord.get(key)+"").compareTo(sqlTerms[i]._objValue+"")>0)) {
											result.get(i).add(currentRecord);
										  } 
										} ; break;
										
									case ">=" :
										
										if (sqlTerms[i]._objValue instanceof Integer) {
											if (key.equals(sqlTerms[i]._strColumnName) && (((Integer)currentRecord.get(key)).intValue()>=(Integer)sqlTerms[i]._objValue)) {
												result.get(i).add(currentRecord);
										    }
									    }
										
										else if (sqlTerms[i]._objValue instanceof Double) {
											if (key.equals(sqlTerms[i]._strColumnName) && (((Double)currentRecord.get(key)).doubleValue()>=(Double)sqlTerms[i]._objValue)) {
												result.get(i).add(currentRecord);
										    }
									    }
										
										else {
										  if (key.equals(sqlTerms[i]._strColumnName) && ((currentRecord.get(key)+"").compareTo(sqlTerms[i]._objValue+"")>=0)) {
											result.get(i).add(currentRecord);
										  } 
										} ; break;
										
										
									case "<" :
										
										if (sqlTerms[i]._objValue instanceof Integer) {
											if (key.equals(sqlTerms[i]._strColumnName) && (((Integer)currentRecord.get(key)).intValue()<(Integer)sqlTerms[i]._objValue)) {
												result.get(i).add(currentRecord);
										    }
									    }
										
										else if (sqlTerms[i]._objValue instanceof Double) {
											if (key.equals(sqlTerms[i]._strColumnName) && (((Double)currentRecord.get(key)).doubleValue()<(Double)sqlTerms[i]._objValue)) {
												result.get(i).add(currentRecord);
										    }
									    }
										
										else {
										  if (key.equals(sqlTerms[i]._strColumnName) && ((currentRecord.get(key)+"").compareTo(sqlTerms[i]._objValue+"")<0)) {
											result.get(i).add(currentRecord);
										  } 
										} ; break;
										
									case "<=" :
										
										if (sqlTerms[i]._objValue instanceof Integer) {
											if (key.equals(sqlTerms[i]._strColumnName) && (((Integer)currentRecord.get(key)).intValue()<=(Integer)sqlTerms[i]._objValue)) {
												result.get(i).add(currentRecord);
										    }
									    }
										
										else if (sqlTerms[i]._objValue instanceof Double) {
											if (key.equals(sqlTerms[i]._strColumnName) && (((Double)currentRecord.get(key)).doubleValue()<=(Double)sqlTerms[i]._objValue)) {
												result.get(i).add(currentRecord);
										    }
									    }
										
										else {
										  if (key.equals(sqlTerms[i]._strColumnName) && ((currentRecord.get(key)+"").compareTo(sqlTerms[i]._objValue+"")<=0)) {
											result.get(i).add(currentRecord);
										  } 
										} ; break;
										
								} //END OF SWITCH
								
							  }
							} //END OF WHILE ENUMERATOR

						} // end of loop counter r

						serialize(p180);
						
					}
					
			
			
			} //END OF GINFO!=NULL

		else {
			for (int i=0;i<sqlTerms.length;i++) {
			
			 Vector<Hashtable<String, Object>> temp=new Vector<Hashtable<String, Object>>();
			 if (sqlTerms[i]._strColumnName.equalsIgnoreCase(selectTable.clusteringKey)) { // PK GIVEN

				String type = null;

				type = sqlTerms[i]._objValue.getClass() + "";

				String location = "";
				// CALL BINARY SEARCH
				if (type.substring(6).equalsIgnoreCase("java.lang.Integer")) {
					System.out.println("is integer");
					cmp=0;
					location = binarySearchInt(selectTable.pageInfoList, 0, selectTable.pageInfoList.size() - 1,
							(Integer) sqlTerms[i]._objValue);

					if (location != null) {
						pageFound = true;
					}

				} else if (type.substring(6).equalsIgnoreCase("java.lang.Double")) {
					System.out.println("is double");
					cmp=1;
					location = binarySearchDouble(selectTable.pageInfoList, 0, selectTable.pageInfoList.size() - 1,
							(Double) sqlTerms[i]._objValue);
					// System.out.println(location);

					if (location != null) {
						pageFound = true;
					}

				}

				else if (type.substring(6).equalsIgnoreCase("java.util.Date")) {
					System.out.println("is date");
					cmp=2;
					location = binarySearchDate(selectTable.pageInfoList, 0, selectTable.pageInfoList.size() - 1,
							(Date) sqlTerms[i]._objValue);
					// System.out.println(location);
					if (location != null) {
						pageFound = true;
					}

				} else if (type.substring(6).equalsIgnoreCase("java.lang.String")) {
					System.out.println("is string");
					cmp=3;
					location = binarySearchString(selectTable.pageInfoList, 0, selectTable.pageInfoList.size() - 1,
							(String) sqlTerms[i]._objValue);
					// System.out.println(location);
					if (location != null) {
						pageFound = true;
					}

				}

				if (pageFound == false)
					throw new DBAppException("THE RECORD YOU WANT TO DELETE DOESN'T EXIST");

				// ------------------------
				Page selectPage = (Page) deserialize(location); // original page
				
				if (sqlTerms[i]._strOperator.equals("=")) {
							
						int recordToSelect = binarySearchPage(selectPage.v, sqlTerms[i]._objValue, selectTable.clusteringKey);
						// delPage.location=location; //--------

						if (recordToSelect == -1) { // NOT FOUND IN ORIGINAL PAGE
							// LOAD OVERFLOWS
							
							int count = 0;
							while (recordToSelect == -1 && count < selectTable.overflowInfoList.size()) {

								if (selectTable.overflowInfoList.get(count).pageName.startsWith(selectPage.pageName + ".")) {
									Page p170 = (Page) deserialize(selectTable.overflowInfoList.get(count).location);
									recordToSelect = binarySearchPage(p170.v, sqlTerms[i]._objValue, selectTable.clusteringKey);

									if (recordToSelect != -1) {
										temp.add(p170.v.get(recordToSelect));
										serialize(p170);
										break;
									}
								}
								count++;
							}

							if (recordToSelect == -1)
								throw new DBAppException("RECORD NOT FOUND");

						} else { // I found the record in orignal page
							temp.add(selectPage.v.get(recordToSelect));
							serialize(selectPage);

						}
						
					
					
					
				} //END OF =
				
				else if (sqlTerms[i]._strOperator.equals(">") || sqlTerms[i]._strOperator.equals(">=")) {
					for (int r = 0; r < selectPage.v.size(); r++) { // IN ONE PAGE
						Hashtable<String, Object> currentRecord = selectPage.v.get(r);

						if(cmp==0) {
							Enumeration<String> e2 = currentRecord.keys();

							//int k = 0;
							while (e2.hasMoreElements()) {
								String key2 = e2.nextElement();
								System.out.println(key2);
								if (key2.equals(sqlTerms[i]._strColumnName) && 
										(((Integer)currentRecord.get(key2)).intValue()<(Integer)sqlTerms[i]._objValue)) {
									//k++;
									break;
								}
								
								else { //GREATER THAN OR EQUALS
									if (sqlTerms[i]._strOperator.equals(">=")  &&
										(((Integer)currentRecord.get(key2)).intValue()>=(Integer)sqlTerms[i]._objValue)) {

										temp.add(currentRecord);

									}
									
									else {
										if (((Integer)currentRecord.get(key2)).intValue()>(Integer)sqlTerms[i]._objValue) {
											temp.add(currentRecord);

										}

									}

								}

							}
						}
						else if(cmp==1) {
							Enumeration<String> e2 = currentRecord.keys();

							//int k = 0;
							while (e2.hasMoreElements()) {
								String key2 = e2.nextElement();
								System.out.println(key2);
								if (key2.equals(sqlTerms[i]._strColumnName) && 
										(((Double)currentRecord.get(key2)).doubleValue()<(Double)sqlTerms[i]._objValue)) {

									//k++;
									break;
								}
								else {
									if (sqlTerms[i]._strOperator.equals(">=")  &&
											(((Double)currentRecord.get(key2)).doubleValue()>=(Double)sqlTerms[i]._objValue)) {

											temp.add(currentRecord);

										}
										
										else {
											if (((Double)currentRecord.get(key2)).doubleValue()>(Double)sqlTerms[i]._objValue) {
												temp.add(currentRecord);

											}

										}
								}

							}
							
						}
						else if(cmp==2) {
							Enumeration<String> e2 = currentRecord.keys();

							int k = 0;
							while (e2.hasMoreElements()) {
								String key2 = e2.nextElement();
								System.out.println(key2);
								if (key2.equals(sqlTerms[i]._strColumnName) && ((currentRecord.get(key2)+"").compareTo((String)sqlTerms[i]._objValue)<0)) {
									//k++;
									break;
								}
								else {
									if (sqlTerms[i]._strOperator.equals(">=")  &&
											(((String)currentRecord.get(key2)).compareTo((String)sqlTerms[i]._objValue))>=0) {

										 temp.add(currentRecord);

										}
										
										else {
											if (((String)currentRecord.get(key2)).compareTo((String)sqlTerms[i]._objValue)>0) {

												temp.add(currentRecord);

											}

										}
								}

							}
							
						}
						else if (cmp==3) {
							Enumeration<String> e2 = currentRecord.keys();

							int k = 0;
							while (e2.hasMoreElements()) {
								String key2 = e2.nextElement();
								System.out.println(key2);
								if (key2.equals(sqlTerms[i]._strColumnName) && ((currentRecord.get(key2)+"").compareTo((String)sqlTerms[i]._objValue)<0)) {
									k++;
									break;
								}
								else {
									if (sqlTerms[i]._strOperator.equals(">=")  &&
											(((String)currentRecord.get(key2)).compareTo((String)sqlTerms[i]._objValue))>=0) {

										 temp.add(currentRecord);

										}
										
										else {
											if (((String)currentRecord.get(key2)).compareTo((String)sqlTerms[i]._objValue)>0) {

												temp.add(currentRecord);

											}

										}
								}
								
							}
							
						}


			  	 } // END OF FOR LOOP 3AL PAGE
					
					for (int g=selectTable.pageInfoList.indexOf(selectPage)+1;g<selectTable.pageInfoList.size();g++) {
						
						Page currentPage = (Page) deserialize(selectTable.pageInfoList.get(g).location);
						for (int k=0;k<currentPage.v.size();k++) {
							Hashtable<String, Object> currentRecord = selectPage.v.get(k);
							temp.add(currentRecord);

						}
						
						for(int l=0;l<selectTable.overflowInfoList.size();l++) {
							if (selectTable.overflowInfoList.get(l).pageName.startsWith(currentPage.pageName + ".")) {
								Page currentOverflow = (Page) deserialize(selectTable.overflowInfoList.get(l).location);
								for (int k=0;k<currentOverflow.v.size();k++) {
									Hashtable<String, Object> currentRecord = currentOverflow.v.get(k);
									temp.add(currentRecord);

								}
								
								serialize(currentOverflow);

							  }
							}

							serialize(currentPage);
						}

					} //END OF >
				
				else if (sqlTerms[i]._strOperator.equals("!=")) {
					
					for (int r = 0; r < selectPage.v.size(); r++) { // IN ONE PAGE
						Hashtable<String, Object> currentRecord = selectPage.v.get(r);

						if(cmp==0) {
							Enumeration<String> e2 = currentRecord.keys();

							//int k = 0;
							while (e2.hasMoreElements()) {
								String key2 = e2.nextElement();
								System.out.println(key2);
								if (key2.equals(selectTable.clusteringKey) && 
										(((Integer)currentRecord.get(key2)).intValue()==(Integer)sqlTerms[i]._objValue)) {
									//k++;
									break;
								}
								
								else { 
									temp.add(currentRecord);
								}

							}
						}
						else if(cmp==1) {
							Enumeration<String> e2 = currentRecord.keys();

							//int k = 0;
							while (e2.hasMoreElements()) {
								String key2 = e2.nextElement();
								System.out.println(key2);
								if (key2.equals(selectTable.clusteringKey) && 
										(((Double)currentRecord.get(key2)).doubleValue()==(Double)sqlTerms[i]._objValue)) {

									//k++;
									break;
								}
								else {
									temp.add(currentRecord);

								}

							}
							
						}
						else if(cmp==2) {
							Enumeration<String> e2 = currentRecord.keys();

							int k = 0;
							while (e2.hasMoreElements()) {
								String key2 = e2.nextElement();
								System.out.println(key2);
								if (key2.equals(selectTable.clusteringKey) && ((currentRecord.get(key2)+"").compareTo((String)sqlTerms[i]._objValue)==0)) {
									//k++;
									break;
								}
								else {
									temp.add(currentRecord);

								}

							}
							
						}
						else if (cmp==3) {
							Enumeration<String> e2 = currentRecord.keys();

							int k = 0;
							while (e2.hasMoreElements()) {
								String key2 = e2.nextElement();
								System.out.println(key2);
								if (key2.equals(selectTable.clusteringKey) && ((currentRecord.get(key2)+"").compareTo((String)sqlTerms[i]._objValue)==0)) {
									k++;
									break;
								}
								else {
									temp.add(currentRecord);

								}
								
							}
							
						}


			  	 } // END OF FOR LOOP 3AL PAGE
					
					for (int g=0;g<selectTable.pageInfoList.size();g++) {
						if (g==selectTable.pageInfoList.indexOf(selectPage)) {
							g++;
						}
						
						Page currentPage = (Page) deserialize(selectTable.pageInfoList.get(g).location);
						for (int k=0;k<currentPage.v.size();k++) {
							Hashtable<String, Object> currentRecord = selectPage.v.get(k);
							temp.add(currentRecord);

						}
						
						for(int l=0;l<selectTable.overflowInfoList.size();l++) {
							if (selectTable.overflowInfoList.get(l).pageName.startsWith(currentPage.pageName + ".")) {
								Page currentOverflow = (Page) deserialize(selectTable.overflowInfoList.get(l).location);
								for (int k=0;k<currentOverflow.v.size();k++) {
									Hashtable<String, Object> currentRecord = currentOverflow.v.get(k);
									temp.add(currentRecord);

								}
								
								serialize(currentOverflow);

							  }
							}

							serialize(currentPage);
						}
					
					
					
				} //END OF !=
				
				else if (sqlTerms[i]._strOperator.equals("<") || sqlTerms[i]._strOperator.equals("<=") ) {
					
					for (int r = 0; r < selectPage.v.size(); r++) { // IN ONE PAGE
						Hashtable<String, Object> currentRecord = selectPage.v.get(r);

						if(cmp==0) {
							Enumeration<String> e2 = currentRecord.keys();

							//int k = 0;
							while (e2.hasMoreElements()) {
								String key2 = e2.nextElement();
								System.out.println(key2);
								if (key2.equals(sqlTerms[i]._strColumnName) && 
										(((Integer)currentRecord.get(key2)).intValue()>(Integer)sqlTerms[i]._objValue)) {
									//k++;
									break;
								}
								
								else { //LESS THAN OR EQUALS
									if (sqlTerms[i]._strOperator.equals("<=")  &&
										(((Integer)currentRecord.get(key2)).intValue()<=(Integer)sqlTerms[i]._objValue)) {

										temp.add(currentRecord);

									}
									
									else {
										if (((Integer)currentRecord.get(key2)).intValue()<(Integer)sqlTerms[i]._objValue) {
											temp.add(currentRecord);

										}

									}

								}

							}
						}
						else if(cmp==1) {
							Enumeration<String> e2 = currentRecord.keys();

							//int k = 0;
							while (e2.hasMoreElements()) {
								String key2 = e2.nextElement();
								System.out.println(key2);
								if (key2.equals(sqlTerms[i]._strColumnName) && 
										(((Double)currentRecord.get(key2)).doubleValue()<(Double)sqlTerms[i]._objValue)) {

									//k++;
									break;
								}
								else {
									temp.add(currentRecord);

								}

							}
							
						}
						else if(cmp==2) {
							Enumeration<String> e2 = currentRecord.keys();

							int k = 0;
							while (e2.hasMoreElements()) {
								String key2 = e2.nextElement();
								System.out.println(key2);
								if (key2.equals(sqlTerms[i]._strColumnName) && ((currentRecord.get(key2)+"").compareTo((String)sqlTerms[i]._objValue)<0)) {
									//k++;
									break;
								}
								else {
									temp.add(currentRecord);

								}
							}	
						}
						else if (cmp==3) {
							Enumeration<String> e2 = currentRecord.keys();

							int k = 0;
							while (e2.hasMoreElements()) {
								String key2 = e2.nextElement();
								System.out.println(key2);
								if (key2.equals(sqlTerms[i]._strColumnName) && ((currentRecord.get(key2)+"").compareTo((String)sqlTerms[i]._objValue)<0)) {
									k++;
									break;
								}
								else {
									temp.add(currentRecord);

								}	
							}	
						}

			  	 } // END OF FOR LOOP 3AL PAGE
					//IF PAGE 0?????? 
					if(selectTable.pageInfoList.indexOf(selectPage)==0) { //PAGE 0
						for(int l=0;l<selectTable.overflowInfoList.size();l++) {
							if (selectTable.overflowInfoList.get(l).pageName.startsWith(selectPage.pageName + ".")) {
								Page currentOverflow = (Page) deserialize(selectTable.overflowInfoList.get(l).location);
								for (int k=0;k<currentOverflow.v.size();k++) {
									Hashtable<String, Object> currentRecord = currentOverflow.v.get(k);
									//CMP???????????????????????????????
									temp.add(currentRecord);

								}
								
								serialize(currentOverflow);

							  }
							}
					}
					
					else {
						for (int g=selectTable.pageInfoList.indexOf(selectPage)-1;g>=0;g--) {
						Page currentPage = (Page) deserialize(selectTable.pageInfoList.get(g).location);
						for (int k=0;k<currentPage.v.size();k++) {
							Hashtable<String, Object> currentRecord = selectPage.v.get(k);
							temp.add(currentRecord);

						}
						
						for(int l=0;l<selectTable.overflowInfoList.size();l++) {
							if (selectTable.overflowInfoList.get(l).pageName.startsWith(currentPage.pageName + ".")) {
								Page currentOverflow = (Page) deserialize(selectTable.overflowInfoList.get(l).location);
								for (int k=0;k<currentOverflow.v.size();k++) {
									Hashtable<String, Object> currentRecord = currentOverflow.v.get(k);
									temp.add(currentRecord);

								}
								
								serialize(currentOverflow);

							  }
							}

							serialize(currentPage);
						}
					
				  }
					
					
				}

			 	
				
			} //END OF PK GIVENN
			 
			else { // PK NOT GIVEN
				//System.out.println("PK NOT GIVENNNN");
				int selected = 0;
				// boolean canSelect = false;
				// boolean canSelectO = false;
				for (int m = 0; m < selectTable.pageInfoList.size(); m++) { // loop on pages of a table
					Page p180 = (Page) deserialize(selectTable.pageInfoList.get(m).location);
					for (int r = 0; r < p180.v.size(); r++) { // IN ONE PAGE
						Hashtable<String, Object> currentRecord = p180.v.get(r);

						Enumeration<String> e130 = currentRecord.keys();
						while (e130.hasMoreElements()) {
							String key = e130.nextElement();

							switch (sqlTerms[i]._strOperator) {
								case "=" : 
									if (key.equals(sqlTerms[i]._strColumnName) && currentRecord.get(key).equals(sqlTerms[i]._objValue)) {
										temp.add(currentRecord);
									} ;break;
									
								case "!=" :
									if (key.equals(sqlTerms[i]._strColumnName) && !(currentRecord.get(key).equals(sqlTerms[i]._objValue))) {
										temp.add(currentRecord);
									} ;break;
									
								case ">" :
									
									if (sqlTerms[i]._objValue instanceof Integer) {
										if (key.equals(sqlTerms[i]._strColumnName) && (((Integer)currentRecord.get(key)).intValue()>(Integer)sqlTerms[i]._objValue)) {
											temp.add(currentRecord);
									    }
								    }
									
									else if (sqlTerms[i]._objValue instanceof Double) {
										if (key.equals(sqlTerms[i]._strColumnName) && (((Double)currentRecord.get(key)).doubleValue()>(Double)sqlTerms[i]._objValue)) {
											temp.add(currentRecord);
									    }
								    }
									
									else {
									  if (key.equals(sqlTerms[i]._strColumnName) && ((currentRecord.get(key)+"").compareTo(sqlTerms[i]._objValue+"")>0)) {
										temp.add(currentRecord);
									  } 
									} ; break;
									
								case ">=" :
									
									if (sqlTerms[i]._objValue instanceof Integer) {
										if (key.equals(sqlTerms[i]._strColumnName) && (((Integer)currentRecord.get(key)).intValue()>=(Integer)sqlTerms[i]._objValue)) {
											temp.add(currentRecord);
									    }
								    }
									
									else if (sqlTerms[i]._objValue instanceof Double) {
										if (key.equals(sqlTerms[i]._strColumnName) && (((Double)currentRecord.get(key)).doubleValue()>=(Double)sqlTerms[i]._objValue)) {
											temp.add(currentRecord);
									    }
								    }
									
									else {
									  if (key.equals(sqlTerms[i]._strColumnName) && ((currentRecord.get(key)+"").compareTo(sqlTerms[i]._objValue+"")>=0)) {
										temp.add(currentRecord);
									  } 
									} ; break;
									
									
								case "<" :
									
									if (sqlTerms[i]._objValue instanceof Integer) {
										if (key.equals(sqlTerms[i]._strColumnName) && (((Integer)currentRecord.get(key)).intValue()<(Integer)sqlTerms[i]._objValue)) {
											temp.add(currentRecord);
									    }
								    }
									
									else if (sqlTerms[i]._objValue instanceof Double) {
										if (key.equals(sqlTerms[i]._strColumnName) && (((Double)currentRecord.get(key)).doubleValue()<(Double)sqlTerms[i]._objValue)) {
											temp.add(currentRecord);
									    }
								    }
									
									else {
									  if (key.equals(sqlTerms[i]._strColumnName) && ((currentRecord.get(key)+"").compareTo(sqlTerms[i]._objValue+"")<0)) {
										  temp.add(currentRecord);
									  } 
									} ; break;
									
								case "<=" :
									
									if (sqlTerms[i]._objValue instanceof Integer) {
										if (key.equals(sqlTerms[i]._strColumnName) && (((Integer)currentRecord.get(key)).intValue()<=(Integer)sqlTerms[i]._objValue)) {
											temp.add(currentRecord);
									    }
								    }
									
									else if (sqlTerms[i]._objValue instanceof Double) {
										if (key.equals(sqlTerms[i]._strColumnName) && (((Double)currentRecord.get(key)).doubleValue()<=(Double)sqlTerms[i]._objValue)) {
											temp.add(currentRecord);
									    }
								    }
									
									else {
									  if (key.equals(sqlTerms[i]._strColumnName) && ((currentRecord.get(key)+"").compareTo(sqlTerms[i]._objValue+"")<=0)) {
										  temp.add(currentRecord);
									  } 
									} ; break;
									
							}
							
							
						}

						selected++;
					} // end of loop counter r

					serialize(p180);
				}

				for (int n = 0; n < selectTable.overflowInfoList.size(); n++) { // loop on pages of a table
					Page p180 = (Page) deserialize(selectTable.overflowInfoList.get(n).location);
					for (int r = 0; r < p180.v.size(); r++) { // IN ONE PAGE
						Hashtable<String, Object> currentRecord = p180.v.get(r);

						Enumeration<String> e130 = currentRecord.keys();
						while (e130.hasMoreElements()) {
							String key = e130.nextElement();

							switch (sqlTerms[i]._strOperator) {
							case "=" : 
								if (key.equals(sqlTerms[i]._strColumnName) && currentRecord.get(key).equals(sqlTerms[i]._objValue)) {
									temp.add(currentRecord);
								} ;break;
								
							case "!=" :
								if (key.equals(sqlTerms[i]._strColumnName) && !(currentRecord.get(key).equals(sqlTerms[i]._objValue))) {
									temp.add(currentRecord);
								} ;break;
								
							case ">" :
								
								if (sqlTerms[i]._objValue instanceof Integer) {
									if (key.equals(sqlTerms[i]._strColumnName) && (((Integer)currentRecord.get(key)).intValue()>(Integer)sqlTerms[i]._objValue)) {
										temp.add(currentRecord);
								    }
							    }
								
								else if (sqlTerms[i]._objValue instanceof Double) {
									if (key.equals(sqlTerms[i]._strColumnName) && (((Double)currentRecord.get(key)).doubleValue()>(Double)sqlTerms[i]._objValue)) {
										temp.add(currentRecord);
								    }
							    }
								
								else {
								  if (key.equals(sqlTerms[i]._strColumnName) && ((currentRecord.get(key)+"").compareTo(sqlTerms[i]._objValue+"")>0)) {
									  temp.add(currentRecord);
								  } 
								} ; break;
								
							case ">=" :
								
								if (sqlTerms[i]._objValue instanceof Integer) {
									if (key.equals(sqlTerms[i]._strColumnName) && (((Integer)currentRecord.get(key)).intValue()>=(Integer)sqlTerms[i]._objValue)) {
										temp.add(currentRecord);
								    }
							    }
								
								else if (sqlTerms[i]._objValue instanceof Double) {
									if (key.equals(sqlTerms[i]._strColumnName) && (((Double)currentRecord.get(key)).doubleValue()>=(Double)sqlTerms[i]._objValue)) {
										temp.add(currentRecord);
								    }
							    }
								
								else {
								  if (key.equals(sqlTerms[i]._strColumnName) && ((currentRecord.get(key)+"").compareTo(sqlTerms[i]._objValue+"")>=0)) {
									  temp.add(currentRecord);
								  } 
								} ; break;
								
								
							case "<" :
								
								if (sqlTerms[i]._objValue instanceof Integer) {
									if (key.equals(sqlTerms[i]._strColumnName) && (((Integer)currentRecord.get(key)).intValue()<(Integer)sqlTerms[i]._objValue)) {
										temp.add(currentRecord);
								    }
							    }
								
								else if (sqlTerms[i]._objValue instanceof Double) {
									if (key.equals(sqlTerms[i]._strColumnName) && (((Double)currentRecord.get(key)).doubleValue()<(Double)sqlTerms[i]._objValue)) {
										temp.add(currentRecord);
								    }
							    }
								
								else {
								  if (key.equals(sqlTerms[i]._strColumnName) && ((currentRecord.get(key)+"").compareTo(sqlTerms[i]._objValue+"")<0)) {
									  temp.add(currentRecord);
								  } 
								} ; break;
								
							case "<=" :
								
								if (sqlTerms[i]._objValue instanceof Integer) {
									if (key.equals(sqlTerms[i]._strColumnName) && (((Integer)currentRecord.get(key)).intValue()<=(Integer)sqlTerms[i]._objValue)) {
										temp.add(currentRecord);
								    }
							    }
								
								else if (sqlTerms[i]._objValue instanceof Double) {
									if (key.equals(sqlTerms[i]._strColumnName) && (((Double)currentRecord.get(key)).doubleValue()<=(Double)sqlTerms[i]._objValue)) {
										temp.add(currentRecord);
								    }
							    }
								
								else {
								  if (key.equals(sqlTerms[i]._strColumnName) && ((currentRecord.get(key)+"").compareTo(sqlTerms[i]._objValue+"")<=0)) {
									  temp.add(currentRecord);
								  } 
								} ; break;
								
						  }
							
									
						}

						selected++;

					} // end of loop counter r
					serialize(p180);
				}

				if (selected == 0) {
					throw new DBAppException("NO RECORDS FOUND");
				}

			} // END OF PK NOT GIVEN
			 
			 result.add(temp);
			 } // END OF LOOP 3AL SQL TERMS

		//Hashtable fifi = new Hashtable();
			
		 }	
			
		Vector<Hashtable<String, Object>> currentV = new Vector<Hashtable<String, Object>>();

		for (int r = 0; r < result.size()-1; r++) {
			String operator = arrayOperators[r];

			if (currentV.size() == 0) {
				currentV.addAll(result.get(r));

			}

			if (currentV.size() == 0 && r<result.size()-1 ) {
				currentV.addAll(result.get(r));

			}

			switch (operator) {
			case "AND":
				if(r<result.size()-1) {
					for (int s = 0; s < currentV.size(); s++) { // LOOP ON RECORDS
					 if (result.get(r + 1).contains(currentV.get(s))) {
						continue;
					} else {
						currentV.remove(currentV.get(s));
						//System.out.println(currentV);
					}

				  }
					
				} ;
				
				break;

			case "OR":
				if(r<result.size()-1) {
    				for (int s = 0; s < result.get(r + 1).size(); s++) { // LOOP ON RECORDS
    					if (!currentV.contains(result.get(r + 1).get(s)))
    						currentV.add(result.get(r + 1).get(s));
    				}

					
				} ;

				
				break;

			case "XOR":

				if(r<result.size()-1) {

				for (int s = 0; s < result.get(r + 1).size(); s++) { // LOOP ON RECORDS
					if (!currentV.contains(result.get(r + 1).get(s)))
						currentV.add(result.get(r + 1).get(s));


					else {
						currentV.remove(result.get(r + 1).get(s)); // SIZE CHANGES???

					}
					
				  } //END OF FOR LOOP
				
				} ;
				
				break;

			}

		} 

		Iterator iterator=currentV.iterator();
		
		return iterator;
	}

	public void createIndex(String tableName, String[] columnNames) throws DBAppException {
		int dimensions = columnNames.length;
		GridInfo gInfo = new GridInfo(columnNames);
		int cmp=-1;
		Table t=(Table) deserialize("src/main/resources/data/" + tableName + ".ser");

		FileWriter temp = null;
		
		for(int i=0;i<dimensions;i++) {
			if(!t.colNameType.containsKey(columnNames[i])) {
				throw new DBAppException("YOU'RE TRYING TO CREATE AN INDEX ON A COLUMN THAT DOESN'T EXIST");

			}
			
		}
		
		//set grid name for ser
		gInfo.gridName=tableName+"-grid-"+t.grids.size();
		
		String [] tmp = columnNames;
		Arrays.sort(tmp);
		for(int p = 0 ;p< t.grids.size();p++){
			Arrays.sort(t.grids.get(p).colNames);

			if(Arrays.equals(tmp,t.grids.get(p).colNames)){
				throw new DBAppException("GRID ALREADY EXISTS");
			}
		}

		
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
		try {
			temp = new FileWriter("src/main/resources/temp.csv");
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		
		String current = null;
		try {
			current = br.readLine();
			//current = br.readLine();

		} catch (IOException e) {
			e.printStackTrace();
		}

		while (current != null) {
			//System.out.println("BBBBBBBBBB");

			String[] line = current.split(",");
			String tName = line[0];
			String colName = line[1];
			String colType = line[2];

			@SuppressWarnings("unused")
			String clusteringKey = line[3];
			String indexed = line[4];
			Object min = line[5];
			Object max = line[6];

			String newLine="";
			if (tableName.equalsIgnoreCase(tName)) {
				//System.out.println("TABLE NAMES EQUAL");
				Object[] mins = null;
				for (int x = 0; x < columnNames.length; x++) {
					mins = new Object[10];
					if ((columnNames[x]).equalsIgnoreCase(colName)) { // COLUMN TO CREATE INDEX ON FOUND
						//System.out.println("COLUMN NAMES EQUAL");
						//load csv
						//rewrite line
						//overwrite csv
						//FileOutputStream out = null;
						
						line[4]="True";
						
						Object interval;
						if (colType.equalsIgnoreCase("java.lang.integer")) {
							interval = (( Integer.parseInt(max+"") - Integer.parseInt(min+"")) / 10) + 1;
							int k = Integer.parseInt(min+"");
							int index = 0;
							while ((k <= Integer.parseInt(max+"")) && (index<10)) {
								mins[index] = k;
								k += Integer.parseInt(interval+"");
								index++;
							}

							gInfo.gridRanges.put(colName, mins);
							cmp = 0;

						} else if (colType.equalsIgnoreCase("java.lang.double")) {
							interval = ( ( Double.parseDouble(max+"") - Double.parseDouble(min+"") ) / 10.0) ;
							double k = Double.parseDouble(min+"");
							int index = 0;
							while ((k <= Double.parseDouble(max+"")) && (index<10)) {
								mins[index] = k;
								k += Double.parseDouble(interval+"");
								index++;
							}

							gInfo.gridRanges.put(colName, mins);
							cmp = 1;

						} else if (colType.equalsIgnoreCase("java.lang.string")) {
							//not necessarily 10 values in range??
							//ArrayList<Character> letters = new ArrayList<Character>();
							
							int c=((String) min).charAt(0);
							
							if((c >=65 && c<=90) || (c>=97 && c<=122)) { //LETTERS
							
								Object[] letters=new Object[10]; //A->Z and a->z so max size=52
								
								//int count=0;
								
								int index = 0;

								char firstMin= ((String)min).charAt(0);
								
								char firstMax= ((String)max).charAt(0);

								interval= (( (int) firstMax - (int) firstMin ) /10) +1; //number of letters
															
								char currentChar=firstMin;
								
								while ((int)currentChar<=((int)firstMax) && (index<10)) {
									letters[index]=currentChar;
									currentChar= (char)((int)currentChar +(Integer)interval) ;
									//count++;
									index++;
										
								}
								
						  	    gInfo.gridRanges.put(colName, letters);
								
							}
							
							else { //NUMBERS 
								String minimum;
								String maximum;
								
								String[] minS = (min+"").split("-"); //[46,1000]
								String[] maxS = (max+"").split("-"); //[49,1000]

								minimum = minS[0]+minS[1]; // 461000
				                maximum = maxS[0]+maxS[1]; //491000
				                
								interval = ( ( Integer.parseInt(maximum) - Integer.parseInt(minimum) ) / 10 ) + 1; //491000-461000

								int k = Integer.parseInt(minimum);
								int index = 0;
								while (k <= Integer.parseInt(maximum) && (index<10)) {
									mins[index] = k;
									k += (int) interval;
									index++;
								}
								
								//mins= [461000,464000,467000,....]
								gInfo.gridRanges.put(colName, mins);
				                
							}
							
							cmp = 2;

						} else if (colType.equalsIgnoreCase("java.util.date")) {
							//System.out.println("DATEEEE");
							
							DateFormat format = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
							Date minD = null;
							Date maxD = null;

							try {
								minD = format.parse(min+"");
								maxD = format.parse(max+"");;
							} catch (ParseException e) {
								e.printStackTrace();
							}
							
							LocalDate minDate = convertToLocalDateViaInstant(minD);
							LocalDate maxDate = convertToLocalDateViaInstant(maxD);

							//System.out.println(minDate);
							//Period total = Period.between(minDate, maxDate);
							long p = ChronoUnit.DAYS.between(minDate, maxDate);
							interval = (long) (p / 10);

							LocalDate k = minDate;

							int index = 0;

							while ((k.compareTo(maxDate) <= 0) && (index<10)) {
								mins[index] = k;
								//k+=(LocalDate)interval;
								k = k.plusDays((long) interval);

								index++;
							}

							gInfo.gridRanges.put(colName, mins);
							cmp = 3;


						}

					}

				} //END OF FOR LOOP ON COLUMNS TO BE INDEXED
	
			}
			

			newLine = line[0] + "," + line[1] + "," + line[2] + "," + line[3] +"," 
					+ line[4]+"," + line[5] +"," + line[6];

			try {
				//System.out.println(newLine);
				//System.out.println(writer);
				temp.append(newLine);
				temp.append("\n");
				current = br.readLine();

			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			
			
		} //END OF WHILE CURRENT!=NULL
			
			try {
				br.close();
				temp.close();

			} catch (IOException e) {
				e.printStackTrace();
			}
			
			
			File f2 = null;
			try {
				f2 = new File("src/main/resources/metadata.csv");
				f2.delete();

			} catch (Exception e) {
				e.printStackTrace();
			}
			
			
			File f3 = new File("src/main/resources/temp.csv");
			f3.renameTo(f2);
			
			t.grids.add(gInfo);
			
			
			//System.out.println("CSV READDD");

			//finished reading csv for all columns and have their intervals in array
			
					for (int n=0;n<t.pageInfoList.size();n++) {
						
						Page p19 = (Page) deserialize(t.pageInfoList.get(n).location);
						for(int r=0;r<p19.v.size();r++) {
							Hashtable<String, Object> currentRecord = p19.v.get(r);
							insertInBucket(tableName,p19.location,gInfo,currentRecord);

						}
						serialize(p19);


					}
					
		for (int n=0;n<t.overflowInfoList.size();n++) {

			Page p19 = (Page) deserialize(t.overflowInfoList.get(n).location);
			for(int r=0;r<p19.v.size();r++) {
				Hashtable<String, Object> currentRecord = p19.v.get(r);
				insertInBucket(tableName,p19.location,gInfo,currentRecord);

			}
			//END FOR LOOP 3ALA ONE PAGE
			serialize(p19);
		}//END OF FOR LOOP 3AL PAGES
		
		serialize(t);

	} //end of creatIndex

	private String getBucketName(String tableName, String[] columnNames, ArrayList<Integer> cellIndices) {
		String bucketName  = tableName+"#";

		for(int i = 0 ; i<columnNames.length;i++){
			bucketName += columnNames[i]+","+cellIndices.get(i);
			if(i!=columnNames.length-1){
				bucketName+="#";
			}

		}
			return bucketName; //tableName#colName0,value0#colName1,value1#....
		//tableName,columnames,values
		//split on "#" first then on "," to get values
	}


	public static LocalDate convertToLocalDateViaInstant(Date dateToConvert) {
		return dateToConvert.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
	}
	
	
	public GridInfo findGrid(Table t,Hashtable<String,Object> values) {
		
		int max=0;
		int current=0;
		
		GridInfo gr=null;
		
		ArrayList<String> insertedColumns=new ArrayList<String>();

		Enumeration<String> e = values.keys();
		while (e.hasMoreElements()) {
			String key = e.nextElement();
			insertedColumns.add(key);	
		}
		
		for(GridInfo k: t.grids) {
			for(int x=0;x<k.colNames.length;x++) { //name
				for(int y=0;y<insertedColumns.size();y++) { //name,,age,gender
					if(insertedColumns.get(y).equalsIgnoreCase(k.colNames[x])) {
						current++;
						break;
					}
					
				}
					
			}
			
			if(current>max) {
				gr=k;
				max=current;
			}
					
			current=0;
			
		}
		
		return gr;	
	}	
} //4444 ma2sooda