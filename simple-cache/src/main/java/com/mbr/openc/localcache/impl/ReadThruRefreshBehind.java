package com.mbr.openc.localcache.impl;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.TimeZone;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.mbr.openc.localcache.DataXferInterface;
import com.mbr.openc.localcache.DataXferInterfaceImpl;
import com.mbr.openc.localcache.SimpleCacheDatasource;
import com.mbr.openc.localcache.utils.SimpleCacheUtils;

public class ReadThruRefreshBehind extends BaseCacheImpl {
	protected Log log = LogFactory.getLog(getClass());
	private static boolean DEBUG = false;
	private Properties config = null;
	private static int CACHE_LIMIT_SIZE = 0;
	private static boolean NO_REHASH=false;
	private static CacheMap<Object, Object> concMap = null;
	private SimpleCacheDatasource dataSrc = null;
	private static LinkedBlockingQueue<DataXferInterface> senderQue = new LinkedBlockingQueue<DataXferInterface>();
	private static LinkedBlockingQueue<DataXferInterface> receiverQue = new LinkedBlockingQueue<DataXferInterface>();
	private static int STATS_SIZE = 15;
	private static Queue<CacheStats> stats = new ArrayDeque<CacheStats>(STATS_SIZE);

	// TIMER TASK
	private Timer timer = null;

	public ReadThruRefreshBehind(SimpleCacheDatasource dataSrc, Properties cacheProps) throws Exception {
		super(cacheProps);
		this.config = cacheProps;
		DEBUG = log.isDebugEnabled();
		this.dataSrc = dataSrc;
		validateProps();
		init();
	}

	@Override
	protected void validateProps() throws Exception {
		super.validateProps();
		log.info("Validating SimpleCache Properties for ReadThruRefreshBehind ");

		if (null == config.getProperty("cache.refresh.interval")
				|| config.getProperty("cache.refresh.interval").equals("")) {
			throw new Exception("Property[cache.refresh.interval] is EMPTY. Cannot Proceed.");
		}

		if (!config.getProperty("cache.refresh.interval").matches("[1-9][0-9]{1,10}")) {
			throw new Exception(
					"Property[cache.refresh.interval] Not Proper. 1) Must be Numeric 2) Min of two digits 3) Cannot exceed 10 digit limit 4) Bare min of 10 seconds");
		}

		// Validate if the datasrc is valid
		if (dataSrc == null)
			throw new Exception("Datasource cannot be NULL in ReadThruRefreshBehind Cache");

		log.info("Completed Validating SimpleCache Properties for ReadThruRefreshBehind ");

	}

	/**
	 * Initialize
	 */
	private void init() {
		log.info("ReadThruRefreshBehind init() Invoked");
		CACHE_LIMIT_SIZE = Integer.parseInt(config.getProperty("cache.limit.size"));
		NO_REHASH = (config.getProperty("cache.with.norehash")!=null && "Y".equals(config.getProperty("cache.with.norehash")))?true:false;

		// get the computed cache config here 
		ComputedCacheConfig cacheCfg = SimpleCacheUtils.calcInitCapacity(CACHE_LIMIT_SIZE, NO_REHASH);

		concMap = new CacheMap<Object, Object>(cacheCfg.getInitCapacity(), cacheCfg.getLoadFac(),cacheCfg.getCacheSize());
		log.info("configProperty [cache.map.capacity] [" + config.getProperty("cache.map.capacity")
				+ "] set to SimpleCache");

		log.info("Scheduling Timer");
		timer = new Timer();
		int secs = Integer.parseInt(config.getProperty("cache.refresh.interval"));
		timer.schedule(new RefreshCacheMap(), 0, secs * 1000);
		log.info("configProperty [cache.refresh.interval] in SECONDS [" + secs + "] set to SimpleCache");
		log.info("Finished Scheduling Timer");

		// Load the Initial List as supplied in the Properties file
		config.keySet().forEach(a -> {
			String x = (String) a;
			if (x.startsWith("cache.load.key")) {
				try {
					this.get(config.getProperty(x));
					log.info("Cache Initialization: Loaded Key [" + config.getProperty(x) + "]");
				} catch (Exception e) {
					log.error("Failed loading Key [" + config.getProperty(x)
							+ "] from Datasource for Cache Initialization of Pre-Loaded Set", e);
				}

				log.info("Initialized Key [" + config.getProperty(x) + "]");
			}
		});

		log.info("ReadThruRefreshBehind init() Invocation Complete");
	}

	@Override
	public Object get(Object key) throws Exception {

		if (DEBUG)
			log.debug("ReadThruRefreshBehind get Called ");
		// TODO: 1 : Check if the Object is in Cache. If so return it
		if (this.exists(key)) {
			if (DEBUG)
				log.debug("ReadThruRefreshBehind returning data from Cache");
			return concMap.get(key);
		}

		// TODO: 2 : if DOES not contain - Load from the DataSource , PUT in
		// Cache and return the Response Data
		DataXferInterface resp;
		try {
			if (DEBUG)
				log.debug("Key[" + key + "] not in Cache. Datasource Callback being invoked");
			resp = dataSrc.getData(new DataXferInterfaceImpl(key, null));
		} catch (Exception e) {
			log.error("Datasource in SimpleCache:ReadThruRefreshBehind threw Exception" + e);
			return null;
		}

		//Check for NULLs here.
		if (resp != null && resp.getResponse()!= null) {
			if (DEBUG)
				log.debug("SimpleCache:ReadThruRefreshBehind get complete after Datasource Callback");
			concMap.put(resp.getRequest(), resp.getResponse());
			return resp.getResponse();
		}else{
			log.error("SimpleCache:ReadThruRefreshBehind DataStore RETURNED NULL for Key["+key+"]. RETURNING NULL");
		}

		if (DEBUG)
			log.debug("SimpleCache:ReadThruRefreshBehind get : Returning NULL becoz Datasource response was NULL");
		return null;
	}

	@Override
	public void put(Object key, Object value) throws Exception {

		if (DEBUG)
			log.debug("SimpleCache:ReadThruRefreshBehind PUT Called ");
		if (null == key) {
			log.error("Key Cannot be NULL. Please check the why the Key is NULL?");
			throw new Exception("Key Cannot be NULL. Please check the why the Key is NULL?");
		}
		if (null == value) {
			log.error(
					"Value Cannot be NULL. NO NULLs allowed in Cache Object [ConcurrentHashMap]. Check for NULLS before calling the \"put\" method");
			throw new Exception(
					"Value Cannot be NULL. NO NULLs allowed in Cache Object [ConcurrentHashMap]. Check for NULLS before calling the \"set\" method");
		}

		concMap.put(key, value);
		if (DEBUG)
			log.debug("ReadThruRefreshBehind PUT Complete ");

	}

	
	@Override
	public Object remove(Object key) {
		if (DEBUG)
			log.debug("SimpleCache:ReadThruRefreshBehind remove Called ");
		return concMap.remove(key);
	}

	@Override
	public boolean exists(Object key) {
		if (DEBUG)
			log.debug("SimpleCache:ReadThruRefreshBehind exists Called ");
		return concMap.keyExists(key);
	}

	@Override
	public int size() {
		if (DEBUG)
			log.debug("SimpleCache:ReadThruRefreshBehind cache size invoked");
		return concMap.size();
	}

	/**
	 * TIMER TASK
	 * 
	 * @author sm58496
	 *
	 */
	private class RefreshCacheMap extends TimerTask {

		@Override
		public void run() {

			log.info("Starting TIMER");
			try {
				// check if the CacheMap has elements
				if (concMap.isEmpty()) {
					log.info("Cache is empty - Nothing to refresh");
					return;
				}
				// CALL Async Task here
				AsyncProcess asyncProcess = new AsyncProcess();
				try {
					if (DEBUG)
						log.debug("Starting Async Process");
					asyncProcess.execute();
					if (DEBUG)
						log.debug("Async Process COMPLETED");
				} catch (Exception e) {
					log.error("AsyncProcess threw Exception." + e);
					log.error("Suppresssing Failure. Will Try again");
				} finally {
					asyncProcess = null;
				}
			} finally {
				if (DEBUG)
					log.debug("finally:Nullified Async Process");
				if (DEBUG)
					log.debug("Current Values in the Conc Map");
				if (DEBUG) {
					concMap.forEach(1, (a, b) -> log.debug("Key[" + a + "]"));
				}
				log.info("Ending TIMER");
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
	 * -------------------------------------------------------------------------
	 * -------------------------------------------------------------------------
	 * A S Y N C H R O N O U S .......T A S K S
	 * -------------------------------------------------------------------------
	 * -------------------------------------------------------------------------
	 */

	/**
	 * 
	 * @author sm58496
	 *
	 */
	private class AsyncProcess {

		public void execute() throws Exception {
			log.info("Async Process execute Start");
			// Start executor service here
			Date startTime = getNow();

			ExecutorService exeServ = Executors.newCachedThreadPool();
			Map<Object, List<String>> grouped = null;
			List<Future<String>> results = null;
			Collection<Callable<String>> tasks = null;

			try {
				tasks = new ArrayList();

				// Submit 1 Sender Task
				tasks.add(new SenderTask());

				// submit 5 Datasource tasks
				// If this design is accepted - put in a formaula to handle this
				// load - based on the CacheMap Size
				tasks.add(new DatasourceTask());
				tasks.add(new DatasourceTask());
				tasks.add(new DatasourceTask());
				tasks.add(new DatasourceTask());
				tasks.add(new DatasourceTask());

				// submit 1 receiver tasks
				tasks.add(new ReceiverTask());

				// Invoke all
				results = exeServ.invokeAll(tasks);
				log.info("AsyncProcess : All Send/Data/Recv tasks submitted");

				exeServ.shutdown();
				log.info("AsyncProcess : ExecutorService sent to Shutdown - Meaning - NO NEW tasks");
				// At this point no more new tasks

				// wait for 1 minute before shutting this whole this
				log.info("AsyncProcess : ExecutorService Waiting for 1 Minute for task to close in orderly process");
				exeServ.awaitTermination(1, TimeUnit.MINUTES);

				// check if all the tasks are done now, IF Not - force it down
				if (!exeServ.isTerminated()) {
					log.info(
							"AsyncProcess : ExecutorService Is not Terminated after awaiting 1 Minute. Giving it another minute ");
					exeServ.awaitTermination(1, TimeUnit.MINUTES);

					if (!exeServ.isTerminated()) {
						log.info(
								"AsyncProcess : ExecutorService Is not Terminated after awaiting 2 Minutes. Force Shutdown Now and wait for another Minute again");
						exeServ.shutdownNow();
						exeServ.awaitTermination(1, TimeUnit.MINUTES);

						if (!exeServ.isTerminated())
							log.error(
									"AsyncProcess : ExecutorService After total elapsed 3 minutes of Shutdown also did not work. Something worong");
					}

				} else {
					log.info("AsyncProcess : ExecutorService SHUTDOWN in orderly process");
				}

				// Check the count of the results
				grouped = results.parallelStream().map(f -> {
					String s = "ERRO";
					try {
						s = f.get();
					} catch (Exception e) {
						e.printStackTrace();
						return s;
					}
					return s.substring(0, 3);

				}).collect(Collectors.groupingBy(a -> a));

				if (DEBUG)
					log.debug("Total Async Tasks RUN [" + tasks.size() + "]");
				if (DEBUG)
					log.debug(
							" >> Legend Starts with 0 (No Work) 1 (Ran) 9 (Ran but Error)  S (Sender) D (Datasource) R (Receiver)");
				if (DEBUG)
					grouped.forEach((k, v) -> {
						log.debug(k + " [" + v.size() + "]");
					});

				// store to stats
				Date endTime = getNow();
				runStats(new CacheStats(grouped, formatDate(startTime), formatDate(endTime),
						getElapsedTimeInSec(startTime, endTime)));
				endTime = null;
				startTime = null;

			} catch (Exception e) {
				log.error("Error in AsyncProcess.execute()", e);
			} finally {
				tasks = null;
				grouped = null;
				results = null;
				exeServ = null;
				if (DEBUG)
					log.debug("Executor Service NULLED");
			}

			log.info("Async Process execute COMPLETE");
		}
	}

	/**
	 * SENDER TASK JUST POPULATES THE SENDER Q
	 * 
	 * @author sm58496
	 *
	 */
	private class SenderTask implements Callable<String> {

		@Override
		public String call() throws Exception {
			long tid = Thread.currentThread().getId();
			if (DEBUG)
				log.debug("Starting Sender Task for TID [" + tid + "]");
			String result = "0#S:NOWORK";

			try {
				if (DEBUG)
					log.debug("Extracting Keys from ConcurrentMap to Set");
				Set<Object> keys = concMap.keySet();
				// load the keys into an array
				Object[] keyArr = keys.toArray();
				keys = null;

				if (null != keyArr)
					log.debug("Total Key count being bused to SenderQ [" + keyArr.length + "]");

				if (DEBUG)
					log.debug("Set Nulled. Keys pulled into Array");
				for (Object obj : keyArr) {

					// check if the Key still exists in the CacheMap 
					// Chances are the Key has expired and has been removed.
					if(!concMap.keyExists(obj)){
						log.info("Key DROPPED at Sender.Could have Expired. Key ["+obj+"]");
						continue;
					}

					// Put it in the Sender Q
					senderQue.add(new DataXferInterfaceImpl(obj, null));
					if (DEBUG)
						log.debug("Key [" + obj + "] put into SenderQue");
				}
				result = "1#S:RAN";

			} catch (Exception e) {
				result = "9#S:" + e.getLocalizedMessage();
				log.error("Failed in SenderTask", e);
			}

			if (DEBUG)
				log.debug("Completed Sender Task for TID [" + tid + "]");
			return result;
		}
	}

	/**
	 * DATASOURCE This Task will read from Sender Q and Involke the Underlying
	 * Datasource and puts the response back into the Receiver Que.
	 * 
	 * @author sm58496
	 *
	 */
	private class DatasourceTask implements Callable<String> {

		@Override
		public String call() throws Exception {

			long tid = Thread.currentThread().getId();
			String result = "0#D:NOWORK";
			if (DEBUG)
				log.debug("Starting Datasource Task for TID [" + tid + "]");

			try {
				DataXferInterface reqObj = null;
				while ((reqObj = senderQue.poll(60, TimeUnit.SECONDS)) != null) {
					DataXferInterface respObj = null;
					
					// check if the Key still exists in the CacheMap 
					// Chances are the Key has expired and has been removed.
					if(!concMap.keyExists(reqObj.getRequest())){
						log.info("Key DROPPED at Datasource.Could have Expired. Key ["+reqObj.getRequest()+"]");
						continue;
					}
					
					// Call DataSource here
					try {
						respObj = dataSrc.getData(reqObj);
					} catch (Exception e) {
						log.error("Datasource in AsyncProcess$SenderTask Failed.. will skip and continue", e);
						continue;
					}

					// Put this Latest Response in the ReceiverQ
					if (null != respObj) {
						receiverQue.add(respObj);
					} else {
						log.error("Response Object for datasource request was NULL. REquest was ["
								+ respObj.getRequest()
								+ "]. Not PUT in REceiverQ. Suppressing it. This Key could be evicted from Cache for No hits");
					}
				}
				result = "1#D:RAN";

			} catch (Exception e) {
				result = "9#D:" + e.getLocalizedMessage();
				log.error("Failed in DatasourceTask.", e);
			}

			if (DEBUG)
				log.debug("Completed Datasource Task for TID [" + tid + "]");
			return result;
		}
	}

	/**
	 * Drains the ReceiverQ and Updates the ConcurrentMap
	 * 
	 * @author sm58496
	 *
	 */
	private class ReceiverTask implements Callable<String> {

		@Override
		public String call() throws Exception {

			long tid = Thread.currentThread().getId();
			String result = "0#R:NOWORK";
			if (DEBUG)
				log.debug("Starting Reciever Task for TID [" + tid + "]");
			try {
				DataXferInterface respObj = null;
				while ((respObj = receiverQue.poll(60, TimeUnit.SECONDS)) != null) {

					// Put the response onto the CacheMap
					if (null != respObj.getRequest() && null != respObj.getResponse()) {

						// check if the Key still exists in the CacheMap 
						// Chances are the Key has expired and has been removed.
						if(!concMap.keyExists(respObj.getRequest())){
							log.info("Key DROPPED at Receiver.Could have Expired. Key ["+respObj.getRequest()+"]");
							continue;
						}

						concMap.put(respObj.getRequest(), respObj.getResponse());
						if (DEBUG)
							log.debug("Added ReceiverQue Obj to ConcMap [" + respObj.getRequest() + ']');
					}
				}
				result = "1#R:RAN";
			} catch (Exception e) {
				result = "9#R:" + e.getLocalizedMessage();
				log.error("Failed in ReceiverTask.", e);
			}
			if (DEBUG)
				log.debug("Completed Reciever Task for TID [" + tid + "]");
			return result;
		}
	}

	private void runStats(CacheStats cacheSts) {
		if (stats.size() == STATS_SIZE) {
			stats.remove();
		}
		stats.add(cacheSts);
		if (DEBUG)
			stats.forEach(a -> log.info(a));
	}

	/**
	 * Store Cache stats
	 */
	private class CacheStats {

		private Map<Object, List<String>> groupedResults;
		private String startTime;
		private String endTime;
		private long elapsedRunTime;

		public CacheStats(Map<Object, List<String>> groupedResults, String startTime, String endTime,
				long elapsedRunTime) {
			super();
			this.groupedResults = groupedResults;
			this.startTime = startTime;
			this.endTime = endTime;
			this.elapsedRunTime = elapsedRunTime;
		}

		@Override
		public String toString() {
			StringBuilder bldr = new StringBuilder();
			bldr.append("{");
			if (groupedResults != null)
				groupedResults.forEach((k, v) -> {
					bldr.append(k + " [" + v.size() + "];");
				});
			bldr.append("}");
			return "CacheStats [groupedResults=" + bldr.toString() + ", startTime=" + startTime + ", endTime=" + endTime
					+ ", elapsedRunTime=" + elapsedRunTime + "]";
		}

	}

	private static java.util.Date getNow() {
		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("America/Chicago"));
		return cal.getTime();
	}

	private static String formatDate(Date dt) {
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss z");
		return df.format(dt);
	}

	private static long getElapsedTimeInSec(Date startDt, Date endDt) {
		long diff = endDt.getTime() - startDt.getTime();
		return diff / 1000;
	}

}
