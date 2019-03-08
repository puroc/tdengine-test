package com.example.data;

import java.util.List;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;

import com.example.component.InsertUnit;
import com.example.component.SeriesDb;
import com.example.component.TimeSeriesData;
import com.example.util.TimeUtil;

public class FlowDataMaker implements IDataMaker{
	
	private long fromTime;

	private int comanpyNum;

	private int factoryNum;

	private int rtuNum;

	private int insertNum4Rtu;
	
	private Timer timer = new Timer();
	
	public FlowDataMaker(long fromTime,int comanpyNum,int factoryNum,int rtuNum,int insertNum4Rtu) {
		this.fromTime = fromTime;
		this.comanpyNum = comanpyNum;
		this.factoryNum = factoryNum;
		this.rtuNum = rtuNum;
		this.insertNum4Rtu = insertNum4Rtu;	
	}

	public void run() {
		final long start = System.currentTimeMillis();
		timer.scheduleAtFixedRate(new TimerTask() {
			
			@Override
			public void run() {
				long expectTotalNum =comanpyNum*factoryNum*rtuNum*insertNum4Rtu;
				long insertTotalNum = 0;
				long sleepTotalNum = 0;
				long insertTotalTime = 0;
				List<InsertUnit> insertUnitList = SeriesDb.getInstance().getInsertUnitList();
				for(InsertUnit unit:insertUnitList) {
					insertTotalNum+=unit.getInsertTotalNum().get();
				}
//				System.out.println("insertTotalNum:"+insertTotalNum+",expectTotalNum:"+expectTotalNum);
				if(insertTotalNum==expectTotalNum) {
					long end = System.currentTimeMillis();
					for(InsertUnit unit:insertUnitList) {
						sleepTotalNum+=unit.getSleepTotalNum().get();
					}
					for(InsertUnit unit:insertUnitList) {
						insertTotalTime+=unit.getInsertTotalTime().get();
					}
					System.out.println("insert "+ insertTotalNum +" rows,total expend:"+(end-start) +" ms.insertTotalTime:"+(insertTotalTime / 1000 / 1000) +",sleepNum:"+sleepTotalNum);
					FlowDataMaker.this.timer.cancel();
				}
			}
		}, 0, 10);
		
		Random random = new Random();
		for (int i = 0; i < comanpyNum; i++) {
			for (int j = 0; j < factoryNum; j++) {
				for (int k = 0; k < rtuNum; k++) {
					String tableName = "flow_" + i + "_" + j + "_" + k;
					long lastFromTime = fromTime;
					for(int z=0;z<insertNum4Rtu;z++) {	
						int accumulatedFlow = z;
						int instantFlow = random.nextInt(10);
						String data = String.format("%d,%d,%d", lastFromTime,accumulatedFlow, instantFlow);
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
	
	

}
