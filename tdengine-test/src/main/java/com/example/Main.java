package com.example;

import java.text.ParseException;

import com.example.config.Config;
import com.example.data.FlowDataMaker;
import com.example.data.MeterDataMaker;
import com.example.table.FlowTableMaker;
import com.example.table.MeterTableMaker;
import com.example.util.DbUtil;
import com.example.util.TimeUtil;

public class Main {

	public static void main(String[] args) {
		try {
			readArgs(args);
			DbUtil.getInstance().createDb();

			long fromTime = TimeUtil.parse("2019-01-01 00:00:00").getTime();

//			FlowTableMaker ftm = new FlowTableMaker();
//			ftm.run();

//			FlowDataMaker fdm = new FlowDataMaker(fromTime, FlowTableMaker.COMPANY_NUM, FlowTableMaker.FACTORY_NUM,
//					FlowTableMaker.RTU_NUM, Config.getInstance().getInsertNumPerRtu());
//			fdm.run();

			MeterTableMaker mtm = new MeterTableMaker();
			mtm.run();

			MeterDataMaker mdm = new MeterDataMaker(fromTime, MeterTableMaker.COMPANY_NUM, MeterTableMaker.METER_NUM,
					Config.getInstance().getInsertNumPerMeter());
			mdm.run();
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}

	private static void readArgs(String[] args) {
		for (int i = 0; i < args.length; i++) {
			if (args[i].startsWith(Config.BATCH_NUM)) {
				Config.getInstance().setBatchNum(Integer.parseInt(args[i].split("=")[1]));
			} else if (args[i].startsWith(Config.THREAD_NUM)) {
				Config.getInstance().setThreadNum(Integer.parseInt(args[i].split("=")[1]));
			} else if (args[i].startsWith(Config.COMPANY_NUM)) {
				Config.getInstance().setCompanyNum(Integer.parseInt(args[i].split("=")[1]));
			} else if (args[i].startsWith(Config.FACTORY_NUM)) {
				Config.getInstance().setFactoryNum(Integer.parseInt(args[i].split("=")[1]));
			} else if (args[i].startsWith(Config.RTU_NUM)) {
				Config.getInstance().setRtuNum(Integer.parseInt(args[i].split("=")[1]));
			} else if (args[i].startsWith(Config.METER_NUM)) {
				Config.getInstance().setMeterNum(Integer.parseInt(args[i].split("=")[1]));
			} else if (args[i].startsWith(Config.INSERT_NUM_PER_RTU)) {
				Config.getInstance().setInsertNumPerRtu(Integer.parseInt(args[i].split("=")[1]));
			} else if (args[i].startsWith(Config.INSERT_NUM_PER_METER)) {
				Config.getInstance().setInsertNumPerMeter(Integer.parseInt(args[i].split("=")[1]));
			} else {
				System.out.println("wrong arg:" + args[i]);
			}
		}
	}

}
