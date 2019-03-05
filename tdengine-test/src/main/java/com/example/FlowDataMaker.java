package com.example;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

public class FlowDataMaker implements DataMaker{

	private static final int DATA_FLOW_TOTAL_NUM = 1;

	private static final int COMPANY_NUM = 2;

	private static final int FACTORY_NUM = 2;

	private static final int RTU_NUM = 2;

	private static final String DB_NAME = "water_db";

	public static final String SQL_CREATE_DB = String.format("create database if not exists %s", DB_NAME);

	public static final String SUPER_TABLE_NAME_FLOW = "flow";

	public static final String SQL_SUPER_TABLE_FLOW = String.format(
			"create table if not exists %s (ts timestamp, forword_flow int ,negative_flow int,instant_flow int) tags(company_id int,factory_id int,device_id nchar(50))",
			"flow");

	public void createAndOpenDb(Connection conn, String createDbSQL) {
		Statement stmt = null;
		try {
			stmt = (Statement) conn.createStatement();
			String sql = SQL_CREATE_DB;
			stmt.executeUpdate(sql);
			System.out.println(sql + " success");
			sql = String.format("use %s", DB_NAME);
			stmt.executeUpdate(sql);
			System.out.println(sql + " success");
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("create db failed");
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("create failed");
		} finally {
			try {
				if (stmt != null)
					stmt.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

	}

	public void createSuperTable(Connection conn, String createTableSQL) {
		Statement stmt = null;
		try {
			stmt = (Statement) conn.createStatement();
			String sql = createTableSQL;
			stmt.executeUpdate(sql);
			System.out.println(sql + " success");
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("create table failed");
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("create table failed");
		} finally {
			try {
				if (stmt != null)
					stmt.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	public String createTable(Connection conn, String[] args) {
		int companyId = Integer.parseInt(args[0]);
		int factoryId= Integer.parseInt(args[1]); 
		int deviceId= Integer.parseInt(args[2]); 
		String tableName = "flow_" + companyId + "_" + factoryId + "_" + deviceId;
		Statement stmt = null;
		try {
			stmt = (Statement) conn.createStatement();
			String sql = String.format(
					"create table if not exists %s using " + SUPER_TABLE_NAME_FLOW + " tags(%d,%d,%s)",
					tableName, companyId, factoryId, deviceId + "");
			stmt.executeUpdate(sql);
			System.out.println(sql + " success");
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("create table failed");
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("create table failed");
		} finally {
			try {
				if (stmt != null)
					stmt.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return tableName;
	}

	public void run() {
		DbUtil.getInstance().connectToTaosd();
		Connection conn = DbUtil.getInstance().getConnection();
		createAndOpenDb(conn, SQL_CREATE_DB);
		createSuperTable(conn, SQL_SUPER_TABLE_FLOW);
		for (int i = 0; i < COMPANY_NUM; i++) {
			for (int j = 0; j < FACTORY_NUM; j++) {
				for (int k = 0; k < RTU_NUM; k++) {
					String[] args = new String[] {i+"",j+"",k+""};
					String tableName = createTable(conn,args);
					insertData(conn,tableName,DATA_FLOW_TOTAL_NUM);
				}
			}
		}		
	}

	public void insertData(Connection conn,String tableName,long totalNum) {
		Statement stmt = null;
		int start = (int) System.currentTimeMillis();
		long rowsInserted =0;
		Random random =  new Random();
		try {
			stmt = (Statement) conn.createStatement();
			for (int i = 0; i < totalNum; i++) {
				int forwordFlow = i;
				int instantFlow = random.nextInt(10);
				String sql = String.format("insert into "+tableName+" values(0,%d,0,%d)", forwordFlow,instantFlow);
					long affectRows = stmt.executeUpdate(sql);
					rowsInserted += affectRows;
				}
			System.out.println("insert "+rowsInserted+ " rows into "+tableName+" success");
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("insert into table failed");
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("insert into table failed");
		} finally {
			try {
				if (stmt != null)
					stmt.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		int end = (int) System.currentTimeMillis();
		System.out.printf("Total %d rows inserted, %d rows failed, time spend %d seconds.\n", rowsInserted,
				totalNum - rowsInserted, (end - start) / 1000);
	}



}
