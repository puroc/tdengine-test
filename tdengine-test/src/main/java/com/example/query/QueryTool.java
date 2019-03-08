package com.example.query;

import com.example.ConnWrapper;
import com.example.util.DbUtil;

public class QueryTool {
	
	public static void queryFlow15(){
		ConnWrapper connWrapper = DbUtil.getInstance().connectToTaosd();
		String sql= "select max(accumulated_flow),first(accumulated_flow),last(accumulated_flow),avg(instant_flow),max(instant_flow),min(instant_flow) from flow where device_id = 1 and ts > '2019-01-01 00:30:00'  and ts < '2019-01-01 00:45:00' group by device_id ;";
		DbUtil.getInstance().query(connWrapper, sql);
		DbUtil.getInstance().closeConn(connWrapper);
	}
	
	public static void main(String[] args) {
		QueryTool.queryFlow15();
	}

}
