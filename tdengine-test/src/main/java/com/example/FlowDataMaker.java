package com.example;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class FlowDataMaker implements DataMaker {

	private static final int DATA_FLOW_TOTAL_NUM = 10;

	private static final int COMPANY_NUM = 2;

	private static final int FACTORY_NUM = 3;

	private static final int RTU_NUM = 10;

	private static final String SUPER_TABLE_NAME_FLOW = "flow";

	private static final String SQL_SUPER_TABLE_FLOW = String.format(
			"create table if not exists %s (ts timestamp, forword_flow int ,negative_flow int,instant_flow int) tags(company_id int,factory_id int,device_id nchar(50))",
			"flow");

	private static final int THREAD_NUM = 1;

	private List<String> tableList = new ArrayList<String>();

	public void createSuperTable(String createTableSQL) {
		ConnectionObj obj = null;
		try {
			obj = DbUtil.getInstance().connectToTaosd();
			String sql = createTableSQL;
			obj.getStmt().executeUpdate(sql);
			System.out.println(sql + " success");
		} catch (Throwable e) {
			e.printStackTrace();
			System.out.println("create table failed");
		} finally {
			DbUtil.getInstance().closeConnectionObj(obj);
		}
	}

	public String createTable(String[] args) {
		int companyId = Integer.parseInt(args[0]);
		int factoryId = Integer.parseInt(args[1]);
		int deviceId = Integer.parseInt(args[2]);
		String tableName = "flow_" + companyId + "_" + factoryId + "_" + deviceId;
		ConnectionObj obj = null;
		try {
			obj = DbUtil.getInstance().connectToTaosd();
			String sql = String.format(
					"create table if not exists %s using " + SUPER_TABLE_NAME_FLOW + " tags(%d,%d,%s)", tableName,
					companyId, factoryId, deviceId + "");
			obj.getStmt().executeUpdate(sql);
			System.out.println(sql + " success");
		} catch (Throwable e) {
			e.printStackTrace();
			System.out.println("create table failed");
		} finally {
			DbUtil.getInstance().closeConnectionObj(obj);
		}
		return tableName;
	}

	public void run() {
		createSuperTable(SQL_SUPER_TABLE_FLOW);
		for (int i = 0; i < COMPANY_NUM; i++) {
			for (int j = 0; j < FACTORY_NUM; j++) {
				for (int k = 0; k < RTU_NUM; k++) {
					String[] args = new String[] { i + "", j + "", k + "" };
					String tableName = createTable(args);
					tableList.add(tableName);
				}
			}
		}
		doInsertData();
	}

	private void doInsertData() {
		Hashtable<Integer, List<String>> tasks = new Hashtable<Integer, List<String>>();
		int tableNum = tableList.size();
		if (tableNum < THREAD_NUM) {
			List<String> tablesPerThread = new ArrayList<String>();
			tablesPerThread.addAll(tableList);
			tasks.put(0, tablesPerThread);
		} else {
			if (tableNum % THREAD_NUM == 0) {
				int numsPerThread = tableNum / THREAD_NUM;
				int index = 0;
				for (int j = 0; j < THREAD_NUM; j++) {
					List<String> tablesPerThread = new ArrayList<String>();
					for (int k = 0; k < numsPerThread; k++) {
						tablesPerThread.add(tableList.get(index + k));
					}
					index+=numsPerThread;
					tasks.put(j, tablesPerThread);
				}
			} else {
				int numsPerThread = tableNum / THREAD_NUM;
				int index = 0;
				for (int j = 0; j < THREAD_NUM; j++) {
					List<String> tablesPerThread = new ArrayList<String>();
					if (j == THREAD_NUM - 1) {
						int num = tableNum % THREAD_NUM;
						for (int k = 0; k < num; k++) {
							tablesPerThread.add(tableList.get(index + k));
						}
					} else {
						for (int k = 0; k < numsPerThread; k++) {
							tablesPerThread.add(tableList.get(index + k));
						}
					}
					index+=numsPerThread;
					tasks.put(j, tablesPerThread);
				}
			}

			for (Entry<Integer, List<String>> entry : tasks.entrySet()) {
				Iterator<String> iterator = entry.getValue().iterator();
				StringBuffer sb = new StringBuffer();
				while (iterator.hasNext()) {
					sb.append(iterator.next()).append(",");
				}
				System.out.println("ThreadNum:" + entry.getKey() + ",list:" + sb.toString());
			}
			
			List<Thread> threadList = new ArrayList<Thread>();
			final AtomicLong rowsInserted = new AtomicLong();
			
			for (final Entry<Integer, List<String>> entry : tasks.entrySet()) {
				System.out.println("ThreadNum:" + entry.getKey()+" begin");
				Thread t = new Thread(new Runnable() {

					public void run() {
						ConnectionObj obj = null;
						try {
							obj = DbUtil.getInstance().connectToTaosd();
						List<String> tables = entry.getValue();
						for(String tableName:tables) {
							rowsInserted.addAndGet(insertData(obj,tableName, DATA_FLOW_TOTAL_NUM));
						}}catch(Throwable t) {
							t.printStackTrace();
						}finally {
							DbUtil.getInstance().closeConnectionObj(obj);
						}
					}
					
				});
				threadList.add(t);
			}
			long start = System.currentTimeMillis();
			
			for(Thread t :threadList) {
				t.start();
			}
			
			for(Thread t :threadList) {
				try {
					t.join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			
			long end = System.currentTimeMillis();
			System.out.printf("Total %d rows inserted, time spend %d seconds.\n", rowsInserted.get(), (end - start) / 1000);
		}
	}

	public long insertData(ConnectionObj obj,String tableName, long totalNum) {
		long rowsInserted = 0;
		Random random = new Random();
		try {
			for (int i = 0; i < totalNum; i++) {
				int forwordFlow = i;
				int instantFlow = random.nextInt(10);
				String sql = String.format("insert into " + tableName + " values(0,%d,0,%d)", forwordFlow, instantFlow);
				long affectRows = obj.getStmt().executeUpdate(sql);
				if(affectRows<=0) {
					System.out.println("insert "+tableName+" failed row:"+affectRows);
				}
				rowsInserted += affectRows;
			}
			System.out.println("insert " + rowsInserted + " rows into " + tableName + " success");
		} catch (Throwable e) {
			e.printStackTrace();
			System.out.println("insert into table failed");
		} 
		return rowsInserted;
	}

}