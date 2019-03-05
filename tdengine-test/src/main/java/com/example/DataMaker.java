package com.example;

import java.sql.Connection;

public interface DataMaker {
	 void createAndOpenDb(Connection conn, String createDbSQL);
	 void createSuperTable(Connection conn, String createTableSQL);
	 String createTable(Connection conn, String[] args);
	 void run();
	 void insertData(Connection conn,String tableName,long totalNum);
}
