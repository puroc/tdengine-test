package com.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DbUtil {
	
	private static final String JDBC_PROTOCAL = "jdbc:TSDB://";
	private static final String TSDB_DRIVER = "com.taosdata.jdbc.TSDBDriver";
	private static String host = "192.168.167.201";
	private static String user = "root";
	private static String password = "taosdata";
	private static int port = 6020;
	private String jdbcUrl = String.format("%s%s:%d/%s?user=%s&password=%s", JDBC_PROTOCAL, host, port, "",
			user, password);
	
	private Connection conn = null;
	
	public static final DbUtil DB_UTIL =  new DbUtil();
	
	private DbUtil() {
		
	}
	
	public static DbUtil getInstance() {
		return DB_UTIL;
	}
	
	public void connectToTaosd() {
		try {
			Class.forName(TSDB_DRIVER);
			if (this.conn == null || this.conn.isClosed()) {
				this.conn = (Connection) DriverManager.getConnection(this.jdbcUrl);
			}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.out.println(e.getMessage());
			System.out.println("get connection from " + this.jdbcUrl + " failed");
			System.exit(4);
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println(e.getMessage());
			System.out.println("get connection from " + this.jdbcUrl + " failed");
			System.exit(4);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(e.getMessage());
			System.out.println("get connection from " + this.jdbcUrl + " failed");
			System.exit(4);
		} finally {
		}
		System.out.println("get connection from " + this.jdbcUrl + " success");
	}
	
	public Connection getConnection() {
		return this.conn;
	}
	
	public void closeConnection() {
		try {
			if (this.conn != null)
				this.conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	


}
