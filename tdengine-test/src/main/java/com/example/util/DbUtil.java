package com.example.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Random;

import com.example.ConnWrapper;
import com.example.component.TimeSeriesData;

public class DbUtil {
	
	private static final String JDBC_PROTOCAL = "jdbc:TSDB://";
	private static final String TSDB_DRIVER = "com.taosdata.jdbc.TSDBDriver";
	public static final String DB_NAME = "water_db";
	private static String host = "192.168.167.201";
	private static String user = "root";
	private static String password = "taosdata";
	private static int port = 6020;
	private String jdbcUrl = String.format("%s%s:%d/%s?user=%s&password=%s", JDBC_PROTOCAL, host, port, "",
			user, password);
	
	private static final String SQL_CREATE_DB = String.format("create database if not exists %s", DB_NAME);
	
	public static final DbUtil DB_UTIL =  new DbUtil();
	
	private DbUtil() {
		
	}
	
	public static DbUtil getInstance() {
		return DB_UTIL;
	}
	
	public void createDb() {
		Connection conn = null;
		Statement stmt = null;
		try {
			Class.forName(TSDB_DRIVER);
			conn= (Connection) DriverManager.getConnection(this.jdbcUrl);
			stmt =  (Statement) conn.createStatement();
			String sql = SQL_CREATE_DB;
			stmt.executeUpdate(sql);
			System.out.println(sql + " success");
		} catch (Throwable e) {
			e.printStackTrace();
			System.out.println("create failed");
		} finally {
				try {
					if (conn != null) {
						conn.close();
					}
					if (stmt != null) {
						stmt.close();
					}
				} catch (SQLException e) {
					e.printStackTrace();
				}
		}

	}
	
	public ConnWrapper connectToTaosd() {
		Connection conn =null;
		Statement stmt = null;
		try {
			Class.forName(TSDB_DRIVER);
			conn= (Connection) DriverManager.getConnection(this.jdbcUrl);
			stmt =(Statement) conn.createStatement();
			String sql = String.format("use %s", DB_NAME);
			stmt.executeUpdate(sql);
//			System.out.println(sql + " success");
		} catch (Throwable e) {
			e.printStackTrace();
			System.out.println("get connection from " + this.jdbcUrl + " failed");
		} finally {
			
		}
//		System.out.println("get connection from " + this.jdbcUrl + " success");
		ConnWrapper obj = new ConnWrapper();
		obj.setConn(conn);
		obj.setStmt(stmt);
		return obj;
	}
	
	
	public void closeConn(ConnWrapper obj) {
		try {
			if (obj.getConn() != null)
				obj.getConn().close();
			if (obj.getStmt() != null)
				obj.getStmt().close();
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}
	
	public int insertData(ConnWrapper connWrapper,List<TimeSeriesData> dataList) {
		int affectRows=0;
		try {
		StringBuilder sb = new StringBuilder("insert into");
		for(TimeSeriesData tsd : dataList) {
			String tableName = tsd.getTableName();
			String data = tsd.getData();
			sb.append(" "+tableName+" ");
			sb.append("values(");
			sb.append(data);
			sb.append(")");
		}
			String sql = sb.toString();
			affectRows = connWrapper.getStmt().executeUpdate(sql);
			System.out.println("insert " + affectRows + " rows success");
		} catch (Throwable e) {
			e.printStackTrace();
			System.out.println("insert into table failed");
		} 
		return affectRows;
	}
	
	
	


}
