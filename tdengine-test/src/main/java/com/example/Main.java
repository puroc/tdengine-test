package com.example;

import com.example.table.FlowTableMaker;
import com.example.util.DbUtil;

public class Main {

	public static void main(String[] args) {
		DbUtil.getInstance().createDb();
		FlowTableMaker tm = new FlowTableMaker();
		tm.run();

	}

}
