package com.example.data;

import java.util.List;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

import com.example.component.InsertUnit;
import com.example.component.SeriesDb;
import com.example.component.TimeSeriesData;
import com.example.table.MeterTableMaker;
import com.example.util.TimeUtil;

public class MeterDataMaker implements IDataMaker{
	
	private long fromTime;

	private int comanpyNum;

	private int meterNum;

	private int insertNum4Meter;
	
	private AtomicInteger num4Print = new AtomicInteger() ;
	
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
				num4Print.incrementAndGet();
				long expectTotalNum =comanpyNum*meterNum*insertNum4Meter;
				long insertTotalNum = 0;
				
				List<InsertUnit> insertUnitList = SeriesDb.getInstance().getInsertUnitList();
				for(InsertUnit unit:insertUnitList) {
					insertTotalNum+=unit.getInsertTotalNum().get();
				}
				if(num4Print.get()==100) {
					num4Print.set(0);
					long sleepTotalTime = 0;
					long insertTotalTime = 0;
					long end = System.currentTimeMillis();
					for(InsertUnit unit:insertUnitList) {
						sleepTotalTime+=unit.getSleepTotalTime().get();
					}
					for(InsertUnit unit:insertUnitList) {
						insertTotalTime+=unit.getInsertTotalTime().get();
					}
					System.out.println("[MeterDataMaker]insertTotalNum:"+insertTotalNum+" rows"
							+ ",expend:"+(end-start)+" ms."
							+ "insertTotalTime:"+(insertTotalTime / 1000 / 1000 / insertUnitList.size()+" ms." ) 	
							+"sleepTotalTime:"+ (sleepTotalTime / insertUnitList.size())+" ms." );
				}

				if(insertTotalNum==expectTotalNum) {
					long sleepTotalTime = 0;
					long insertTotalTime = 0;
					long end = System.currentTimeMillis();
					for(InsertUnit unit:insertUnitList) {
						sleepTotalTime+=unit.getSleepTotalTime().get();
					}
					for(InsertUnit unit:insertUnitList) {
						insertTotalTime+=unit.getInsertTotalTime().get();
					}
					System.out.println("insert "+ insertTotalNum 
							+" rows,total expend:"+(end-start) 
							+" ms.insertTotalTime:"+(insertTotalTime / 1000 / 1000 / insertUnitList.size() ) 
							+" ms.sleepTotalTime:"+ (sleepTotalTime / insertUnitList.size())+" ms.");
					MeterDataMaker.this.timer.cancel();
					for(InsertUnit unit:insertUnitList) {
						unit.clearCount();
					}
				}
			}
		}, 0, 10);
		
		Random random = new Random();
		for (int i = 0; i < comanpyNum; i++) {
				for (int k = 0; k < meterNum; k++) {
					String tableName = MeterTableMaker.SUPER_TABLE_NAME_METER+"_" + i + "_" + k;
					long lastFromTime = fromTime;
					for(int z=0;z<insertNum4Meter;z++) {	
						int reading = z;
						String data = String.format("%d,%d", lastFromTime,reading);
						TimeSeriesData tsd =new TimeSeriesData();
						tsd.setTableName(tableName);
						tsd.setData(data);
						SeriesDb.getInstance().insert(tsd);
						lastFromTime = TimeUtil.addSecond(lastFromTime , 30);
					}
				}
		}
	}

	public AtomicInteger getNum4Print() {
		return num4Print;
	}

	public void setNum4Print(AtomicInteger num4Print) {
		this.num4Print = num4Print;
	}
}
