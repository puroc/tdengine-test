package com.example;

public class Main {

	public static void main(String[] args) {
		DbUtil.getInstance().createDb();
		FlowDataMaker tm = new FlowDataMaker();
		tm.run();

	}

}
