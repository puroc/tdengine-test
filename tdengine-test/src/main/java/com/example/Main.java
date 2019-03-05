package com.example;

import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;

import com.example.data.FlowDataMaker;
import com.example.table.FlowTableMaker;
import com.example.util.DbUtil;
import com.example.util.TimeUtil;

public class Main {

	private static final int NUM_PER_RTU = 10000;

	public static void main(String[] args) {
		try {
			DbUtil.getInstance().createDb();
			FlowTableMaker ftm = new FlowTableMaker();
			ftm.run();
			Date date = TimeUtil.parse("2019-01-01 00:00:00");
			FlowDataMaker fdm = new FlowDataMaker(date, FlowTableMaker.COMPANY_NUM, FlowTableMaker.FACTORY_NUM,
					FlowTableMaker.RTU_NUM, NUM_PER_RTU);
			fdm.run();
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}

}
