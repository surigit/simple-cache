package com.mbr.openc.localcache.impl;

import java.util.Calendar;
import java.util.Properties;
import java.util.TimeZone;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.mbr.openc.localcache.SimpleCacheDatasource;
import com.mbr.openc.localcache.SimpleCacheKey;
import com.mbr.openc.localcache.SimpleCacheWithExpiry;

public class ReadThruRefreshAndExpireBehind extends ReadThruRefreshBehind implements SimpleCacheWithExpiry{
	protected Log log = LogFactory.getLog(getClass());
	private static boolean DEBUG = false;
	private static BlockingQueue<CacheExpiry> delayQ = new DelayQueue();
	// TIMER TASK
	private Timer timer = null;

	public ReadThruRefreshAndExpireBehind(SimpleCacheDatasource dataSrc, Properties cacheProps) throws Exception {
		super(dataSrc, cacheProps);
		DEBUG = log.isDebugEnabled();
		init();
	}

	private void init() throws Exception {
		log.info("Scheduling Timer Deamon for Expiry Items");
		timer = new Timer();
		timer.schedule(new ExpiryDaemon(), 0, 1 * 1000);
	}

	public Object get(SimpleCacheKey key) throws Exception {
		Object o = super.get(key.getCacheKey());
		if(o!= null && key.getExpiryTimeInSecs()>0) delayQ.add(new CacheExpiry(key));
		if(o ==null){
			log.error("SimpleCacheKey key ["+key+"] RETURNED NULL response. Nothing to Expire");
		}
		return o;
	}

	public Object remove(SimpleCacheKey key) throws Exception {
		Object o = super.remove(key.getCacheKey());
		if (o!= null) return key;
		return null;
	}

	
	@Override
	public Object get(Object key) throws Exception {
		throw new Exception ("This method signature not to be used for Type : ReadThruRefreshAndExpireBehind");
	}

	/**
	 * EXPIRY DAEMON 
	 * @author sm58496
	 *
	 */
	private class ExpiryDaemon extends TimerTask {

		@Override
		public void run() {
			try {
				//log.info("Running Daemon");
				CacheExpiry expObj = null;
				while ((expObj = delayQ.poll(1, TimeUnit.SECONDS)) != null) {
					if (DEBUG)
						log.debug("CacheKey expired is " + expObj);
					// remove from the map
					try {
						Object removed = ReadThruRefreshAndExpireBehind.this.remove(expObj.getCacheKey());
						if(removed == null) log.warn("NOTHING WAS REMOVED ["+expObj.getCacheKey()+"]");
					} catch (Exception e) {
						log.error("Error in Expiry Daemon while removing key ["+expObj+"].Suppressing it",e);
					}
				}
				//Thread.sleep(1000);
			} catch (InterruptedException e) {
				log.error("ExpiryDaemon was Interrupted..Suppressing it",e);
			}
		}

		@Override
		public boolean cancel() {
			// TODO Auto-generated method stub
			return super.cancel();
		}

		@Override
		public long scheduledExecutionTime() {
			// TODO Auto-generated method stub
			return super.scheduledExecutionTime();
		}

	}

	/**
	 * Inner Class which will expire items in the DelayQueue
	 * 
	 * @author sm58496
	 *
	 */
	private class CacheExpiry implements Delayed {
		private String TIME_ZONE = "America/Chicago";
		private SimpleCacheKey cacheKey = null;
		private long expireAt = 0;

		public CacheExpiry(SimpleCacheKey key) {
			this.cacheKey = key;
			this.expireAt = getExpiryTime(key.getExpiryTimeInSecs());
		}

		@Override
		public int compareTo(Delayed o) {
			CacheExpiry that = (CacheExpiry) o;
			return (int) (this.getElapsedDiff() - that.getElapsedDiff());
		}

		@Override
		public long getDelay(TimeUnit unit) {
			return getElapsedDiff();
		}

		
		public SimpleCacheKey getCacheKey() {
			return cacheKey;
		}

		public String toString() {
			return "[CacheExpiryKey=" + cacheKey.getCacheKey() + ",expireInSecs=" + cacheKey.getExpiryTimeInSecs()
					+ "]";
		}

		private long getExpiryTime(int timeInSecs) {
			Calendar cal = Calendar.getInstance(TimeZone.getTimeZone(TIME_ZONE));
			cal.add(cal.SECOND, timeInSecs);
			return cal.getTime().getTime();
		}

		private long getCurrentTime() {
			Calendar cal = Calendar.getInstance(TimeZone.getTimeZone(TIME_ZONE));
			return cal.getTime().getTime();
		}

		public long getElapsedDiff() {
			return this.expireAt - getCurrentTime();
		}

	}

}
