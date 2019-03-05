package com.example.data;

import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import com.example.component.InsertUnit;
import com.example.component.SeriesDb;
import com.example.component.TimeSeriesData;
import com.example.util.TimeUtil;

public class FlowDataMaker implements IDataMaker{
	
	private Date fromDate;

	private int comanpyNum;

	private int factoryNum;

	private int rtuNum;

	private int insertNum4Rtu;
	
	private Timer timer = new Timer();
	
	public FlowDataMaker(Date fromDate,int comanpyNum,int factoryNum,int rtuNum,int insertNum4Rtu) {
		this.fromDate = fromDate;
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
				List<InsertUnit> insertUnitList = SeriesDb.getInstance().getInsertUnitList();
				for(InsertUnit unit:insertUnitList) {
					insertTotalNum+=unit.getInsertTotalNum().get();
				}
//				System.out.println("insertTotalNum:"+insertTotalNum+",expectTotalNum:"+expectTotalNum);
				if(insertTotalNum==expectTotalNum) {
					long end = System.currentTimeMillis();
					System.out.println("insert "+ insertTotalNum +" rows,expend "+(end-start) +" ms");
					FlowDataMaker.this.timer.cancel();
				}
			}
		}, 0, 10);
		
		Random random = new Random();
		for (int i = 0; i < comanpyNum; i++) {
			for (int j = 0; j < factoryNum; j++) {
				for (int k = 0; k < rtuNum; k++) {
					String tableName = "flow_" + i + "_" + j + "_" + k;
					for(int z=0;z<insertNum4Rtu;z++) {					
						String time = TimeUtil.format(fromDate);
						int forwordFlow = z;
						int instantFlow = random.nextInt(10);
						String data = String.format("'%s',%d,0,%d", time,forwordFlow, instantFlow);
						TimeSeriesData tsd =new TimeSeriesData();
						tsd.setTableName(tableName);
						tsd.setData(data);
						SeriesDb.getInstance().insert(tsd);
						fromDate = TimeUtil.addSecond(fromDate , 30);
					}
				}
			}
		}
	}
	
	

}
