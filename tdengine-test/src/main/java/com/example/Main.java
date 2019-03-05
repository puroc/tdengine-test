package com.example;

import java.util.Date;

import com.example.data.FlowDataMaker;
import com.example.table.FlowTableMaker;
import com.example.util.DbUtil;

public class Main {

	private static final int NUM_PER_RTU = 10000;

	public static void main(String[] args) {
		DbUtil.getInstance().createDb();
		FlowTableMaker ftm = new FlowTableMaker();
		ftm.run();
		Date date = new Date(2019,1,1,0,0,0);
		FlowDataMaker  fdm = new FlowDataMaker(date, FlowTableMaker.COMPANY_NUM, FlowTableMaker.FACTORY_NUM, FlowTableMaker.RTU_NUM, NUM_PER_RTU);
		fdm.run();
	}

}
