package com.example.component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class SeriesDb {
	
	private AtomicInteger timerExecuteNum =  new AtomicInteger();
	
	public static final SeriesDb SERIES_DB = new SeriesDb();
	private SeriesDb () {
		for(int i=0;i<threadNum ;i++) {
			InsertUnit insertUnit = new InsertUnit("unit-"+i,100,10);
			getInsertUnitList().add(insertUnit);
			insertUnit.start();
		}	
		insertUnitSize =getInsertUnitList().size();
	}
	public static final SeriesDb getInstance() {
		return SERIES_DB;
	}
	private List<InsertUnit> insertUnitList = new ArrayList<InsertUnit>();
	
	private int insertUnitSize;
	
	private int threadNum =5;
	
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
	public AtomicInteger getTimerExecuteNum() {
		return timerExecuteNum;
	}
	public void setTimerExecuteNum(AtomicInteger timerExecuteNum) {
		this.timerExecuteNum = timerExecuteNum;
	}
	
 
}
