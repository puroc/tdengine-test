package com.example.table;

public interface ITableMaker {
	 void createSuperTable(String createTableSQL);
	 String createTable(String[] args);
	 void run();
}
