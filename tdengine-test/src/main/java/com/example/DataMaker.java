package com.example;

public interface DataMaker {
	 void createSuperTable(String createTableSQL);
	 String createTable(String[] args);
	 void run();
	 long insertData(ConnectionObj obj,String tableName,long totalNum);
}
