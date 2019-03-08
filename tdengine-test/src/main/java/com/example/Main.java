package com.example;

import java.text.ParseException;
import com.example.config.Config;
import com.example.data.FlowDataMaker;
import com.example.table.FlowTableMaker;
import com.example.util.DbUtil;
import com.example.util.TimeUtil;

public class Main {

	public static void main(String[] args) {
		try {
			readArgs(args);
			DbUtil.getInstance().createDb();
			FlowTableMaker ftm = new FlowTableMaker();
			ftm.run();
			long fromTime = TimeUtil.parse("2019-01-01 00:00:00").getTime();
			FlowDataMaker fdm = new FlowDataMaker(fromTime, FlowTableMaker.COMPANY_NUM, FlowTableMaker.FACTORY_NUM,
					FlowTableMaker.RTU_NUM, Config.getInstance().getInsertNumPerRtu());
			fdm.run();
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}

	private static void readArgs(String[] args) {
		for(int i =0;i<args.length;i++) {
			if(args[i].startsWith(Config.BATCH_NUM)) {
				Config.getInstance().setBatchNum(Integer.parseInt(args[i].split("=")[1]));
			}else if(args[i].startsWith(Config.THREAD_NUM)) {
				Config.getInstance().setThreadNum(Integer.parseInt(args[i].split("=")[1]));
			}else if(args[i].startsWith(Config.COMPANY_NUM)) {
				Config.getInstance().setCompanyNum(Integer.parseInt(args[i].split("=")[1]));
			}else if(args[i].startsWith(Config.FACTORY_NUM)) {
				Config.getInstance().setFactoryNum(Integer.parseInt(args[i].split("=")[1]));
			}else if(args[i].startsWith(Config.RTU_NUM)) {
				Config.getInstance().setRtuNum(Integer.parseInt(args[i].split("=")[1]));
			}else if(args[i].startsWith(Config.INSERT_NUM_PER_RUT)) {
				Config.getInstance().setInsertNumPerRtu(Integer.parseInt(args[i].split("=")[1]));
			}else {
				System.out.println("wrong arg:"+args[i]);
			}
		}
	}

}
