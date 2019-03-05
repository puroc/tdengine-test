package com.example.component;

import java.util.ArrayList;
import java.util.List;

public class SeriesDb {
	
	public static final SeriesDb SERIES_DB = new SeriesDb();
	private SeriesDb () {}
	public static final SeriesDb getInstance() {
		return SERIES_DB;
	}
	private List<InsertUnit> insertUnitList = new ArrayList<InsertUnit>();
	
	private int insertUnitSize =insertUnitList.size();
	
	private int threadNum =1;
	{
		for(int i=0;i<threadNum ;i++) {
			insertUnitList.add(new InsertUnit(100,15*1000));
		}	
	}
	
	//select a insertUnit and add the data into it
	public synchronized void insert(TimeSeriesData tsd) {
		int hashCode = tsd.getTableName().hashCode();
		int index =  hashCode % insertUnitSize;
		InsertUnit insertUnit =insertUnitList.get(index);
		insertUnit.add(tsd);
	}
	
 
}
