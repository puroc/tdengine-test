package com.example.table;

import com.example.ConnWrapper;
import com.example.config.Config;
import com.example.util.DbUtil;

public class MeterTableMaker implements ITableMaker {

	public static final String SUPER_TABLE_NAME_METER = "meter";
	
	public static final int COMPANY_NUM = Config.getInstance().getCompanyNum();
	
	public static final int METER_NUM = Config.getInstance().getRtuNum();
	
	private static final String SQL_SUPER_TABLE_METER = String.format(
			"create table if not exists %s (ts timestamp, reading int) tags(company_id int,device_id nchar(50))",
			SUPER_TABLE_NAME_METER);

	public void createSuperTable(String createTableSQL) {
		ConnWrapper obj = null;
		try {
			obj = DbUtil.getInstance().connectToTaosd();
			String sql = createTableSQL;
			obj.getStmt().executeUpdate(sql);
			System.out.println(sql + " success");
		} catch (Throwable e) {
			e.printStackTrace();
			System.out.println("create table failed");
		} finally {
			DbUtil.getInstance().closeConn(obj);
		}
	}

	public String createTable(String[] args) {
		int companyId = Integer.parseInt(args[0]);
		int deviceId = Integer.parseInt(args[1]);
		String tableName = "meter_" + companyId + "_" + deviceId;
		ConnWrapper obj = null;
		try {
			obj = DbUtil.getInstance().connectToTaosd();
			String sql = String.format("create table if not exists %s using " + SUPER_TABLE_NAME_METER + " tags(%d,%s)",
					tableName, companyId, deviceId + "");
			obj.getStmt().executeUpdate(sql);
			System.out.println(sql + " success");
		} catch (Throwable e) {
			e.printStackTrace();
			System.out.println("create table failed");
		} finally {
			DbUtil.getInstance().closeConn(obj);
		}
		return tableName;

	}

	public void run() {
		createSuperTable(SQL_SUPER_TABLE_METER);
		for (int i = 0; i < COMPANY_NUM; i++) {
				for (int k = 0; k <METER_NUM; k++) {
					String[] args = new String[] { i + "", k + "" };
					createTable(args);
				}
		}
	}

}
