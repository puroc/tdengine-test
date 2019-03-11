package com.example.data;

import java.util.List;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import com.example.component.InsertUnit;
import com.example.component.SeriesDb;
import com.example.component.TimeSeriesData;
import com.example.util.TimeUtil;

public class MeterDataMaker implements IDataMaker{
	
	private long fromTime;

	private int comanpyNum;

	private int meterNum;

	private int insertNum4Meter;
	
	private Timer timer = new Timer();
	
	public MeterDataMaker(long fromTime,int comanpyNum,int meterNum,int insertNum4Meter) {
		this.fromTime = fromTime;
		this.comanpyNum = comanpyNum;
		this.meterNum = meterNum;
		this.insertNum4Meter = insertNum4Meter;	
	}

	public void run() {

		final long start = System.currentTimeMillis();
		timer.scheduleAtFixedRate(new TimerTask() {
			
			@Override
			public void run() {
				long expectTotalNum =comanpyNum*meterNum*insertNum4Meter;
				long insertTotalNum = 0;
				long sleepTotalTime = 0;
				long insertTotalTime = 0;
				List<InsertUnit> insertUnitList = SeriesDb.getInstance().getInsertUnitList();
				for(InsertUnit unit:insertUnitList) {
					insertTotalNum+=unit.getInsertTotalNum().get();
				}
//				System.out.println("insertTotalNum:"+insertTotalNum+",expectTotalNum:"+expectTotalNum);
				if(insertTotalNum==expectTotalNum) {
					long end = System.currentTimeMillis();
					for(InsertUnit unit:insertUnitList) {
						sleepTotalTime+=unit.getSleepTotalTime().get();
					}
					for(InsertUnit unit:insertUnitList) {
						insertTotalTime+=unit.getInsertTotalTime().get();
					}
					System.out.println("insert "+ insertTotalNum +" rows,total expend:"+(end-start) +" ms.insertTotalTime:"+(insertTotalTime / 1000 / 1000 / insertUnitList.size() ) +" ms.sleepTotalTime:"+ (sleepTotalTime / insertUnitList.size())+" ms.");
					MeterDataMaker.this.timer.cancel();
				}
			}
		}, 0, 10);
		
		Random random = new Random();
		for (int i = 0; i < comanpyNum; i++) {
				for (int k = 0; k < meterNum; k++) {
					String tableName = "flow_" + i + "_" + j + "_" + k;
					long lastFromTime = fromTime;
					for(int z=0;z<insertNum4Meter;z++) {	
						int reading = z;
						String data = String.format("%d,%d,%d", lastFromTime,reading);
						TimeSeriesData tsd =new TimeSeriesData();
						tsd.setTableName(tableName);
						tsd.setData(data);
						SeriesDb.getInstance().insert(tsd);
						lastFromTime = TimeUtil.addSecond(lastFromTime , 30);
					}
				}
		}
	}
}
