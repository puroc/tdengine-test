package com.example;

import java.sql.Connection;
import java.sql.Statement;

public class ConnWrapper{
		private Connection conn =null;
		private Statement stmt = null;
		public Connection getConn() {
			return conn;
		}
		public void setConn(Connection conn) {
			this.conn = conn;
		}
		public Statement getStmt() {
			return stmt;
		}
		public void setStmt(Statement stmt) {
			this.stmt = stmt;
		}
		
	}