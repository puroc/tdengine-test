package com.example.config;


public class Config {
	
	private static final Config CONFIG = new Config();
	private Config() {}
	public static final Config getInstance() {
		return CONFIG;
	}
	
	public static final String BATCH_NUM = "batchNum";
	public static final String THREAD_NUM="threadNum";
	public static final String COMPANY_NUM="companyNum";
	public static final String FACTORY_NUM="factoryNum";
	public static final String RTU_NUM="rtuNum";
	public static final String INSERT_NUM_PER_RUT="insertNumPerRtu";
	
	private int batchNum;
	private int threadNum;
	private int companyNum;
	private int factoryNum;
	private int rtuNum;
	private int insertNumPerRtu;
	
	public int getBatchNum() {
		return batchNum;
	}
	public void setBatchNum(int batchNum) {
		this.batchNum = batchNum;
	}
	public int getThreadNum() {
		return threadNum;
	}
	public void setThreadNum(int threadNum) {
		this.threadNum = threadNum;
	}
	public int getCompanyNum() {
		return companyNum;
	}
	public void setCompanyNum(int companyNum) {
		this.companyNum = companyNum;
	}
	public int getFactoryNum() {
		return factoryNum;
	}
	public void setFactoryNum(int factoryNum) {
		this.factoryNum = factoryNum;
	}
	public int getRtuNum() {
		return rtuNum;
	}
	public void setRtuNum(int rtuNum) {
		this.rtuNum = rtuNum;
	}
	public int getInsertNumPerRtu() {
		return insertNumPerRtu;
	}
	public void setInsertNumPerRtu(int insertNumPerRtu) {
		this.insertNumPerRtu = insertNumPerRtu;
	}
	

}
