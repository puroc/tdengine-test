package com.example.component;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import com.example.ConnWrapper;
import com.example.util.DbUtil;

public class InsertUnit {

	private AtomicLong insertTotalNum = new AtomicLong();
	
	private AtomicLong sleepTotalTime = new AtomicLong();
	
	private AtomicLong insertTotalTime = new AtomicLong();

	private Object lock = new Object();

	private int batchNum;

	private int insertInterval;

	private String unitName;

	public InsertUnit(String unitName, int batchNum, int insertInterval) {
		this.setUnitName(unitName);
		this.batchNum = batchNum;
		this.insertInterval = insertInterval;
	}

	private AtomicInteger num = new AtomicInteger();

	private AtomicBoolean isWorking = new AtomicBoolean();

	private Timer timer = new Timer();

	private Thread thread = new Thread(new InsertTask());

	private ConnWrapper connWrapper;

	private List<TimeSeriesData> dataList = new ArrayList<TimeSeriesData>();

	private ConcurrentLinkedQueue<TimeSeriesData> queue = new ConcurrentLinkedQueue<TimeSeriesData>();

	// add the data to this insert unit
	public void add(TimeSeriesData tsd) {
		queue.add(tsd);
	}

	// start insert unit
	public void start() {
		System.out.println(this.getUnitName()+" is starting..");
		connWrapper = DbUtil.getInstance().connectToTaosd();
		timer.schedule(new InsertTimerTask(), 1000, this.insertInterval);
		thread.start();
	}

	public void stop() {
		DbUtil.getInstance().closeConn(connWrapper);
		timer.cancel();
		thread.interrupt();
	}

	// insert the timeSeries data into the timeSeries db
	private int insert(List<TimeSeriesData> dataList) {
		int affectRows = 0;
		try {
			affectRows = DbUtil.getInstance().insertData(connWrapper, dataList);
			System.out.println(this.getUnitName() + " insert " + affectRows + " rows success");
		} catch (Throwable t) {
			t.printStackTrace();
		}
		return affectRows;
	}

	// execute batch insert and rest the dataList and counter(num)
	private void batchInsert(List<TimeSeriesData> dataList) {
		long start = System.nanoTime();
		int affectRows = insert(dataList);
		long end = System.nanoTime();
		long time = end - start;
		this.insertTotalTime.addAndGet(time);
		this.insertTotalNum.addAndGet(affectRows);
		dataList.clear();
		num.set(0);
	}

	public AtomicLong getInsertTotalNum() {
		return this.insertTotalNum;
	}

	public void setInsertTotalNum(AtomicLong insertTotalNum) {
		this.insertTotalNum = insertTotalNum;
	}

	public AtomicLong getSleepTotalTime() {
		return sleepTotalTime;
	}

	public void setSleepTotalTime(AtomicLong sleepTotalTime) {
		this.sleepTotalTime = sleepTotalTime;
	}

	public AtomicLong getInsertTotalTime() {
		return insertTotalTime;
	}

	public void setInsertTotalTime(AtomicLong insertTotalTime) {
		this.insertTotalTime = insertTotalTime;
	}

	public String getUnitName() {
		return unitName;
	}

	public void setUnitName(String unitName) {
		this.unitName = unitName;
	}
	
	public void clearCount() {
		this.getInsertTotalNum().set(0);
		this.getInsertTotalTime().set(0);
		this.getSleepTotalTime().set(0);
	}

	class InsertTimerTask extends TimerTask {

		@Override
		public void run() {
			try {
				// if the insert thread is not working and the dataList is not empty , then this
				// timer will insert the data
				synchronized (lock) {
					if (!isWorking.get() && !dataList.isEmpty()) {
						batchInsert(dataList);
					}
				}
			} catch (Throwable e) {
				e.printStackTrace();
			}
		}
	}

	class InsertTask implements Runnable {

		public void run() {
			try {
				// pull the data and batch insert
				while (true) {
					// when queue is empty , sleep 1 ms
					if (queue.isEmpty()) {
						try {
							isWorking.set(false);
							sleepTotalTime.incrementAndGet();
							Thread.sleep(1);
						} catch (Throwable e) {
							e.printStackTrace();
						}
					} else {
						synchronized (lock) {
							isWorking.set(true);
							// add data to the dataList until num equal batchNum
							if (num.get() < batchNum) {
								dataList.add(queue.poll());
								num.incrementAndGet();
							} else {
								// execute batch insert data
								batchInsert(dataList);
							}
						}
					}
				}
			} catch (Throwable e) {
				e.printStackTrace();
			}

		}

	}

}
