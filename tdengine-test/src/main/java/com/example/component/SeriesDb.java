package com.example.component;

import java.util.ArrayList;
import java.util.List;

import com.example.config.Config;

public class SeriesDb {
	
	private int threadNum = Config.getInstance().getThreadNum();
	
	public static final SeriesDb SERIES_DB = new SeriesDb();
	
	private SeriesDb () {
		for(int i=0;i<threadNum ;i++) {
			InsertUnit insertUnit = new InsertUnit("unit-"+i,Config.getInstance().getBatchNum(),10);
			getInsertUnitList().add(insertUnit);
			insertUnit.start();
		}	
		insertUnitSize = getInsertUnitList().size();
	}
	
	public static final SeriesDb getInstance() {
		return SERIES_DB;
	}
	
	private List<InsertUnit> insertUnitList = new ArrayList<InsertUnit>();
	
	private int insertUnitSize;
	
	//select a insertUnit and add the data into it
	public synchronized void insert(TimeSeriesData tsd) {
		int hashCode = tsd.getTableName().hashCode();
		int index =  hashCode % insertUnitSize;
		InsertUnit insertUnit =getInsertUnitList().get(index);
		insertUnit.add(tsd);
	}
	public List<InsertUnit> getInsertUnitList() {
		return insertUnitList;
	}
	public void setInsertUnitList(List<InsertUnit> insertUnitList) {
		this.insertUnitList = insertUnitList;
	}
}
