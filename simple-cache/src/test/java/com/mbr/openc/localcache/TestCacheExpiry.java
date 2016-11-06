package com.mbr.openc.localcache;

import java.util.Calendar;
import java.util.TimeZone;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

public class TestCacheExpiry {

	BlockingQueue<CacheObject> dq = new DelayQueue(); 

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void test() throws Exception{

		//add objects to the dq
		dq.add(new CacheObject("John",30)); // expire in 30 secs 
		dq.add(new CacheObject("Doe",10)); // expire in 10 secs 
		dq.add(new CacheObject("Martin",20)); // expire in 20 secs 
		dq.add(new CacheObject("Sue",5)); // expire in 5 secs 
		dq.add(new CacheObject("Ken",15)); // expire in 15 secs 
		dq.add(new CacheObject("Mary",23)); // expire in 23 secs 
		dq.add(new CacheObject("George",8)); // expire in 8 secs 

		
		ExecutorService service = Executors.newFixedThreadPool(1);
		service.submit(new MyCallable());
		service.shutdown();
		service.awaitTermination(2, TimeUnit.MINUTES);

		
	}

	private class CacheObject implements Delayed{
		
		private String TIME_ZONE = "America/Chicago";
		private String cacheData = null;
		private long timeToExpire = 0;
		private long expireAt = 0;
		
		CacheObject(String toCache, long expireInSecs){
			this.cacheData = toCache;
			this.timeToExpire = expireInSecs;
			this.expireAt = getExpiryTime((int)expireInSecs); 
		}

		
		public String toString(){
			return "[cacheData="+cacheData+",expireInSecs="+timeToExpire+"]";
		}
		
		
		@Override
		public int compareTo(Delayed o) {
			CacheObject co = (CacheObject)o;
			return (int) (this.getElapsedDiff() - co.getElapsedDiff());
		}

		@Override
		public long getDelay(TimeUnit unit) {
			return getElapsedDiff();
		}
		
		private long getExpiryTime(int timeInSecs){
			Calendar cal = Calendar.getInstance(TimeZone.getTimeZone(TIME_ZONE));
			cal.add(cal.SECOND, timeInSecs);
			return cal.getTime().getTime();
		}

		private long getCurrentTime(){
			Calendar cal = Calendar.getInstance(TimeZone.getTimeZone(TIME_ZONE));
			return cal.getTime().getTime();
		}

		public long getElapsedDiff(){
			return this.expireAt - getCurrentTime();
		}
		
	}

	
	
	private class MyCallable implements Callable<String>{

		@Override
		public String call() throws Exception {

			int x = 0;
			while(x < 100){

				Object c = null;
				while((c= dq.poll(1, TimeUnit.SECONDS))!= null){
					System.out.println("Expired = "+c);
				}
				
				// sleep here 
				Thread.sleep(1000);
				//System.out.println(x);
				x++;
			}
			return null;
		}
		
	}

}
