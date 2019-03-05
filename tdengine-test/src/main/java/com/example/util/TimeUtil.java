package com.example.util;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class TimeUtil {
	
	public static Date addSecond(Date fromDate,int amount) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(fromDate);
		calendar.add(Calendar.SECOND, 30);
		return calendar.getTime();
	}
	
	public static String format(Date date) {
		 SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		 return sdf.format(date);
	}
	
	public static void main(String[] args) {
		  Date date = new Date();
		  System.out.println(TimeUtil.format(date));
		  System.out.println(TimeUtil.format(TimeUtil.addSecond(date,30)));
	}

}
