package com.example.data;

import java.util.Random;

import com.example.component.SeriesDb;
import com.example.component.TimeSeriesData;

public class FlowDataMaker implements IDataMaker{

	private static final int TOTAL_NUM = 10;
	
	private static final int COMPANY_NUM = 2;

	private static final int FACTORY_NUM = 3;

	private static final int RTU_NUM = 10;

	public void run() {
		Random random = new Random();
		for (int i = 0; i < COMPANY_NUM; i++) {
			for (int j = 0; j < FACTORY_NUM; j++) {
				for (int k = 0; k < RTU_NUM; k++) {
					String tableName = "flow_" + i + "_" + j + "_" + k;
					for(int z=0;z<TOTAL_NUM;z++) {
						int forwordFlow = z;
						int instantFlow = random.nextInt(10);
						String data = String.format("now,%d,0,%d", forwordFlow, instantFlow);
						TimeSeriesData tsd =new TimeSeriesData();
						tsd.setTableName(tableName);
						tsd.setData(data);
						SeriesDb.getInstance().insert(tsd);
					}
				}
			}
		}
	}

}
