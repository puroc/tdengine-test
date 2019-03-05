package com.example.component;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.example.ConnWrapper;
import com.example.util.DbUtil;

public class InsertUnit {

	private int batchNum;

	private int insertInterval;

	public InsertUnit(int batchNum, int insertInterval) {
		this.batchNum = batchNum;
		this.insertInterval = insertInterval;
	}

	private AtomicInteger num = new AtomicInteger();

	private AtomicBoolean isWorking = new AtomicBoolean();

	private Timer timer = new Timer();

	private Thread thread = new Thread();

	private List<TimeSeriesData> dataList = new ArrayList<TimeSeriesData>();

	private ConcurrentLinkedQueue<TimeSeriesData> queue = new ConcurrentLinkedQueue<TimeSeriesData>();

	// add the data to this insert unit
	public void add(TimeSeriesData tsd) {
		queue.add(tsd);
	}

	// start insert unit
	public void start() {
		timer.schedule(new InsertTimerTask(), 1000, this.insertInterval);
		thread.start();
	}

	// insert the timeSeries data into the timeSeries db
	private void insert(List<TimeSeriesData> dataList) {
		ConnWrapper connWrapper = null;
		try {
			connWrapper = DbUtil.getInstance().connectToTaosd();
			DbUtil.getInstance().insertData(connWrapper, dataList);
		} catch (Throwable t) {
			t.printStackTrace();
		} finally {
			DbUtil.getInstance().closeConn(connWrapper);
		}
	}

	// execute batch insert and rest the dataList and counter(num)
	private synchronized void batchInsert(List<TimeSeriesData> dataList) {
		insert(dataList);
		dataList.clear();
		num.set(0);
	}

	class InsertTimerTask extends TimerTask {

		@Override
		public void run() {
			try {
				if (!isWorking.get()) {
					batchInsert(dataList);
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
							Thread.sleep(1);
						} catch (Throwable e) {
							e.printStackTrace();
						}
					} else {
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
			} catch (Throwable e) {
				e.printStackTrace();
			}

		}

	}

}
