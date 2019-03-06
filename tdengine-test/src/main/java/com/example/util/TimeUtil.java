package com.example.util;

import java.text.ParseException;
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
	
	public static long addSecond(long time,int amount) {
		return time+amount*1000;
	}
	
	public static String format(Date date) {
		 SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		 return sdf.format(date);
	}
	
	public static Date parse(String time) throws ParseException {
		 SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		 return sdf.parse(time);
	}
	
	public static void main(String[] args) throws ParseException {
		  Date date1 = parse("2019-1-1 00:00:00");
		  long d1 = date1.getTime();
		  long d2 =  addSecond(d1,30);	  
		  System.out.println(d1);
		  System.out.println(d2);
		  System.out.println(d2-d1);
	}

}
