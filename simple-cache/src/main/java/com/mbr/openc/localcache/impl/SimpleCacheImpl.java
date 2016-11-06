package com.mbr.openc.localcache.impl;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.TimeZone;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.mbr.openc.localcache.DataXferInterface;
import com.mbr.openc.localcache.DataXferInterfaceImpl;
import com.mbr.openc.localcache.SimpleCache;
import com.mbr.openc.localcache.SimpleCacheDatasource;

@Deprecated
public class SimpleCacheImpl implements SimpleCache {

	protected Log log = LogFactory.getLog(getClass());

	private SimpleCacheDatasource dataSrc = null;
	private static MyCacheMap<Object, Object> concMap = null;
	private static Semaphore SENTRY = new Semaphore(1, true);
	private static LinkedBlockingQueue<DataXferInterface> senderQue = new LinkedBlockingQueue<DataXferInterface>();
	private static LinkedBlockingQueue<DataXferInterface> receiverQue = new LinkedBlockingQueue<DataXferInterface>();
	private static int STATS_SIZE=15;
	private static Queue<CacheStats> stats = new ArrayDeque<CacheStats>(STATS_SIZE);
	private static Properties config = null;
	private static int CACHE_LIMIT_SIZE=0;
	private static boolean AUTO_REFRESH = true;
	private static boolean DEBUG = false;
	
	// START THE TIMER TASK
	private Timer timer = null;

	public SimpleCacheImpl() {

	}

	/**
	 * Constructor to Load DataSource, along with Supplied Properties. If
	 * Properties are supplied, such Properties will override the defaults.
	 * @param dataSource
	 * @param prop
	 */
	public SimpleCacheImpl(SimpleCacheDatasource dataSource, Properties prop, boolean autoRefresh) throws Exception {

		DEBUG = log.isDebugEnabled();
		AUTO_REFRESH = autoRefresh;
		if (null == dataSource) {
			throw new Exception("Datasource CANNOT be NULL");
		}
		this.dataSrc = dataSource;
		
		if(null == prop || prop.isEmpty())loadDefault();
		
		config = prop;

		validateProps();
		
		init();
	}

	/**
	 * One Arg Constructor, to load with default Properties
	 * 1) AUTO_REFRESH IS TURNED OFF 
	 * 2) ONLY A STATIC CACHE - Cache Updates are out of scope and updates to the cache are via the "set" method.
	 * 
	 * @param dataSource
	 */
	public SimpleCacheImpl(Properties prop) throws Exception {

		DEBUG = log.isDebugEnabled();
		AUTO_REFRESH = false;

		if(null == prop || prop.isEmpty())loadDefault();

		config= prop;
		
		validateProps();

		init();
	}

	/**
	 * Load Default Properties from the SimpleCache.properties from the
	 * Classpath
	 */
	private void loadDefault() throws Exception {

		config = new Properties();

		try (BufferedInputStream bufI = new BufferedInputStream(
				Thread.currentThread().getContextClassLoader().getResourceAsStream("SimpleCache.properties"))) {

			log.info("Loading Default Properties - SimpleCache.properties from the Classpath");
			config.load(bufI);
			log.info(config);
			log.info("Loaded Default Properties - SimpleCache.properties");

		} catch (IOException e) {
			log.error("loadDefault() method failed. Failed to load SimpleCache.properties", e);
			throw new Exception("loadDefault() method failed. Failed to load SimpleCache.properties", e);
		}

	}

	/**
	 * Validate Properties
	 * @throws Exception
	 */
	private void validateProps() throws Exception {
		log.info("Validating SimpleCache Properties ");

		if (null == config.getProperty("cache.map.capacity") || config.getProperty("cache.map.capacity").equals("")) {
			throw new Exception("Property[cache.map.capacity] is EMPTY. Cannot Proceed.");
		}

		if (!config.getProperty("cache.map.capacity").matches("[1-9][0-9]{1,3}")) {
			throw new Exception(
					"Property[cache.map.capacity] has NON-Numeric Characters OR exceeded the 4 digit limit. Anything Beyond 9999 is a Bad Design to use this Cache Strategy.");
		}

		if (null == config.getProperty("cache.limit.size") || config.getProperty("cache.limit.size") .equals("")) {
			throw new Exception("Property[cache.limit.size] is EMPTY. Cannot Proceed.");
		}

		if (!config.getProperty("cache.limit.size").matches("[1-9][0-9]{1,3}")) {
			throw new Exception(
					"Property[cache.limit.size] has NON-Numeric Characters OR exceeded the 4 digit limit. Anything Beyond 9999 is a Bad Design to use this Cache Strategy.");
		}
		if (new Integer(config.getProperty("cache.limit.size")).intValue() >= new Integer(config.getProperty("cache.map.capacity")).intValue()){
			throw new Exception(
					"Property[cache.limit.size] CANNOT BE GREATER THAN or even Equal to [cache.map.capacity]. Your Map InitialCapcity Math needs Correction. Cannot Proceed");
		}

		if(AUTO_REFRESH){
			if (null == config.getProperty("cache.refresh.interval") || config.getProperty("cache.refresh.interval").equals("")) {
				throw new Exception("Property[cache.refresh.interval] is EMPTY. Cannot Proceed.");
			}

			if (!config.getProperty("cache.refresh.interval").matches("[1-9][0-9]{1,10}")) {
				throw new Exception(
						"Property[cache.refresh.interval] Not Proper. 1) Must be Numeric 2) Min of two digits 3) Cannot exceed 10 digit limit");
			}
		}
		
		log.info("Completed Validating SimpleCache Properties ");
	}
	
	
	/**
	 * Initialize 
	 */
	private void init() {

		log.info("init() Invoked");
		CACHE_LIMIT_SIZE = Integer.parseInt(config.getProperty("cache.limit.size"));
		concMap = new MyCacheMap<Object, Object>(Integer.parseInt(config.getProperty("cache.map.capacity")), 0.90f);
		log.info("configProperty [cache.map.capacity] ["+config.getProperty("cache.map.capacity")+"] set to SimpleCache");

		if(!AUTO_REFRESH) log.info("Auto Refresh of SimpleCache is TURNED OFF");

		if(AUTO_REFRESH){
			log.info("Scheduling Timer");
			timer = new Timer();
			int secs = Integer.parseInt(config.getProperty("cache.refresh.interval"));
			timer.schedule(new RefreshCacheMap(), 0, secs * 1000);
			log.info("configProperty [cache.refresh.interval] in SECONDS [" + secs + "] set to SimpleCache");
			log.info("Finished Scheduling Timer");
		}

		// Load the Initial List now 
		config.keySet().forEach(a -> {
			String x = (String)a;
			if (x.startsWith("cache.load.key")){

				try {
					this.get(config.getProperty(x));
					log.info("Cache Initialization: Loaded Key ["+config.getProperty(x)+"]");
				} catch (Exception e) {
					log.error("Failed loading Key ["+config.getProperty(x)+"] from Datasource for Cache Initialization of Pre-Loaded Set",e);
				}
				
				log.info("Initialized Key ["+config.getProperty(x)+"]");
			}
		});
		
		log.info("init() Invocation Complete");
	}

	
	@Override
	public void put(Object key, Object value) throws Exception{
		if(DEBUG) log.debug("SimpleCache PUT Called ");
		if(null==key){
			log.error("Key Cannot be NULL. Please check the why the Key is NULL?");
			throw new Exception ("Key Cannot be NULL. Please check the why the Key is NULL?");
		}
		if(null==value){
			log.error("Value Cannot be NULL. NO NULLs allowed in Cache Object [ConcurrentHashMap]. Check for NULLS before calling the \"put\" method");
			throw new Exception ("Value Cannot be NULL. NO NULLs allowed in Cache Object [ConcurrentHashMap]. Check for NULLS before calling the \"set\" method");
		}
		
		concMap.put(key,value);
		if(DEBUG) log.debug("SimpleCache PUT Complete ");
	}

	
	@Override
	public Object remove(Object key) {
		if(DEBUG) log.debug("SimpleCache REMOVE Called ");
		return concMap.remove(key);
	}

	@Override
	public boolean exists(Object key) {
		if(DEBUG) log.debug("SimpleCache exists Called ");
		return concMap.containsKey(key);
	}

	@Override
	public Object get(Object key) throws Exception{

		if(DEBUG) log.debug("SimpleCache get Called ");
		// TODO: 1 : Check if the Object is in Cache. If so return it
		if (concMap.containsKey(key)) {
			if(DEBUG) log.debug("SimpleCache returning data from Cache");
			return concMap.get(key);
		}

		// if there is NO Datasource - this is a Static Cache 
		if(null == dataSrc) return null;
		
		// TODO: 2 : if DOES not contain - Load from the DataSource , PUT in
		// Cache and return the Response Data
		DataXferInterface resp;
		try {
			if(DEBUG)log.debug("Key[" + key + "] not in Cache. Datasource Callback being invoked");
			resp = dataSrc.getData(new DataXferInterfaceImpl(key,null));
		} catch (Exception e) {
			log.error("Datasource in SimpleCache threw Exception" + e);
			return null;
		}

		if (resp != null) {
			if(DEBUG) log.debug("SimpleCache get complete after Datasource Callback");
			concMap.put(resp.getRequest(), resp.getResponse());
			return resp.getResponse();
		}

		if(DEBUG) log.debug("SimpleCache get : Returning NULL");
		return null;
	}

	@Override
	public int size() {
		if(DEBUG) log.debug("SimpleCache cache size invoked");
		return concMap.size();
	}

	private class MyCacheMap<K, V> extends ConcurrentHashMap<Object, Object> {

		CacheMap<Object, Object> expMap = null;

		public MyCacheMap(int initialCapacity, float loadFactor, int concurrencyLevel) {
			super(initialCapacity, loadFactor, concurrencyLevel);
			// TODO Auto-generated constructor stub
			expMap = new CacheMap(initialCapacity, loadFactor, true,CACHE_LIMIT_SIZE);
		}

		public MyCacheMap(int initialCapacity, float loadFactor) {
			super(initialCapacity, loadFactor);
			// TODO Auto-generated constructor stub
			expMap = new CacheMap(initialCapacity, loadFactor, true,CACHE_LIMIT_SIZE);
		}

		public MyCacheMap(int initialCapacity) {
			super(initialCapacity);
			// TODO Auto-generated constructor stub
			expMap = new CacheMap(initialCapacity, 0.75f, true,CACHE_LIMIT_SIZE);
		}

		// override methods here for PUT ONLY
		@Override
		public Object put(Object key, Object value) {
			Object prevObj = super.put(key, value);

			try {
				// TODO : Acquire Semaphore permit here
				SENTRY.acquire();
				// System.out.println("Acquired Semaphore Permit");
				// TODO : Put the object in the LinkedHashMap
				// Maintain only the Keys here
				expMap.put(key, "DUMMY");
				// TODO : Check if an Object is evicted here
				if (null != expMap.getRemovedKey()) {
					this.remove(expMap.getRemovedKey());
				}
			} catch (Exception e) {
				// TODO: handle exception
				log.error("In CacheMap Put - while handling Expiry Map" + e);
			} finally {
				SENTRY.release();
			}
			return prevObj;
		}

		/**
		 * While get- also invoke the get on the LinkedHashMap - which governs the access order 
		 */
		@Override
		public Object get(Object key) {
			if(super.get(key)!= null){
				expMap.get(key);
			}
			return super.get(key);
		}

		
		
	}

	private class CacheMap<K, V> extends LinkedHashMap<Object, Object> {

		private Object removedKey = null;
		private int SIZE_LIMIT = 10;

		public CacheMap() {
			super();
			// TODO Auto-generated constructor stub
		}

		public CacheMap(int initialCapacity, float loadFactor, boolean accessOrder, int sizeLimit) {
			super(initialCapacity, loadFactor, accessOrder);
			// TODO Auto-generated constructor stub
			SIZE_LIMIT = sizeLimit;
		}


		@Override
		protected boolean removeEldestEntry(Entry<Object, Object> eldest) {

			if(DEBUG)log.debug(size()+"###"+SIZE_LIMIT);
			if (size()>SIZE_LIMIT) {
				removedKey = eldest.getKey();
				if(DEBUG)log.debug("Removing Key[" + removedKey + "]");
				return true;
			}
			
			removedKey = null;

			return false;
		}

		public Object getRemovedKey() {
			return removedKey;
		}

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
					if(DEBUG)log.debug("Starting Async Process");
					asyncProcess.execute();
					if(DEBUG)log.debug("Async Process COMPLETED");
				} catch (Exception e) {
					log.error("AsyncProcess threw Exception." + e);
					log.error("Suppresssing Failure. Will Try again");
				} finally {
					asyncProcess = null;
				} 
			} finally {
				if(DEBUG)log.debug("finally:Nullified Async Process");
				if(DEBUG)log.debug("Current Values in the Conc Map");
				concMap.forEach(1, (a, b) -> log.debug("Key[" + a + "]"));
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
			Map<Object,List<String>> grouped = null;
			List<Future<String>> results = null;
			Collection<Callable<String>> tasks = null;

			try {
				tasks = new ArrayList();

				// Submit 1 Sender Task 
				tasks.add(new SenderTask());
				
				// submit 5 Datasource tasks
				// If this design is accepted - put in a formaula to handle this load - based on the CacheMap Size
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
					log.info("AsyncProcess : ExecutorService Is not Terminated after awaiting 1 Minute. Giving it another minute ");
					exeServ.awaitTermination(1, TimeUnit.MINUTES);

					if (!exeServ.isTerminated()) {
						log.info("AsyncProcess : ExecutorService Is not Terminated after awaiting 2 Minutes. Force Shutdown Now and wait for another Minute again");
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
					
				}).collect(Collectors.groupingBy(a -> a ));
			
				if(DEBUG)log.debug("Total Async Tasks RUN ["+tasks.size()+"]");
				if(DEBUG)log.debug(" >> Legend Starts with 0 (No Work) 1 (Ran) 9 (Ran but Error)  S (Sender) D (Datasource) R (Receiver)");
				if(DEBUG)grouped.forEach((k,v) -> {log.debug(k +" ["+v.size()+"]");});
				
				// store to stats
				Date endTime = getNow();
				runStats(new CacheStats(grouped, formatDate(startTime), formatDate(endTime), getElapsedTimeInSec(startTime, endTime)));
				endTime = null;
				startTime = null;
			
			} catch (Exception e) {
				log.error("Error in AsyncProcess.execute()",e);
			}finally{
				tasks = null;
				grouped = null;
				results = null;
				exeServ = null;
				if(DEBUG)log.debug("Executor Service NULLED");
			}

			log.info("Async Process execute COMPLETE");
		}
	}

	/**
	 * SENDER TASK JUST POPULATES THE SENDER Q 
	 * @author sm58496
	 *
	 */
	private class SenderTask implements Callable<String> {

		@Override
		public String call() throws Exception {
			long tid = Thread.currentThread().getId();
			if(DEBUG)log.debug("Starting Sender Task for TID ["+tid+"]");
			String result="0#S:NOWORK";
			
			try {
				if(DEBUG)log.debug("Extracting Keys from ConcurrentMap to Set");
				Set<Object> keys = concMap.keySet();
				// load the keys into an array
				Object[] keyArr = keys.toArray();
				keys = null;

				if(null!= keyArr)log.debug("Total Key count being bused to SenderQ ["+keyArr.length+"]");
				
				if(DEBUG)log.debug("Set Nulled. Keys pulled into Array");
				for (Object obj : keyArr) {
					// Put it in the Sender Q
					senderQue.add(new DataXferInterfaceImpl(obj, null));
					if(DEBUG)log.debug("Key [" + obj + "] put into SenderQue");
				}
				result = "1#S:RAN";

			} catch (Exception e) {
				result = "9#S:"+e.getLocalizedMessage();
				log.error("Failed in SenderTask",e);
			}

			if(DEBUG)log.debug("Completed Sender Task for TID ["+tid+"]");
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
			String result="0#D:NOWORK";
			if(DEBUG)log.debug("Starting Datasource Task for TID ["+tid+"]");
			
			try {
				DataXferInterface reqObj = null;
				while ((reqObj = senderQue.poll(60, TimeUnit.SECONDS)) != null) {
					DataXferInterface respObj = null;
					// Call DataSource here
					try {
						respObj = dataSrc.getData(reqObj);
					} catch (Exception e) {
						log.error("Datasource in AsyncProcess$SenderTask Failed.. will skip and continue",e);
						continue;
					}

					// Put this Latest Response in the ReceiverQ
					if (null != respObj) {
						receiverQue.add(respObj);
					}else{
						log.error("Response Object for datasource request was NULL. REquest was ["+respObj.getRequest()+"]. Not PUT in REceiverQ. Suppressing it. This Key could be evicted from Cache for No hits");
					}
				}
				result = "1#D:RAN";

			} catch (Exception e) {
				result = "9#D:"+e.getLocalizedMessage();
				log.error("Failed in DatasourceTask.",e);
			}

			if(DEBUG)log.debug("Completed Datasource Task for TID ["+tid+"]");
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
			String result="0#R:NOWORK";
			if(DEBUG)log.debug("Starting Reciever Task for TID ["+tid+"]");
			try {
				DataXferInterface respObj = null;
				while ((respObj = receiverQue.poll(60, TimeUnit.SECONDS)) != null) {

					// Put the response onto the CacheMap
					if (null != respObj.getRequest() && null != respObj.getResponse()) {
						concMap.put(respObj.getRequest(), respObj.getResponse());
						if(DEBUG)log.debug("Added ReceiverQue Obj to ConcMap [" + respObj.getRequest() + ']');
					}
				}
				result = "1#R:RAN";
			} catch (Exception e) {
				result = "9#R:"+e.getLocalizedMessage();
				log.error("Failed in ReceiverTask.",e);
			}
			if(DEBUG)log.debug("Completed Reciever Task for TID ["+tid+"]");
			return result;
		}
	}

	
	private void runStats(CacheStats cacheSts){
		if(stats.size() == STATS_SIZE){
			stats.remove();
		}
		stats.add(cacheSts);
		if(DEBUG)stats.forEach(a -> log.info(a));
	}
	
	
	/**
	 * Store Cache stats 
	 */
	private class CacheStats{
		
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
			if(groupedResults!=null)groupedResults.forEach((k,v) -> {bldr.append(k +" ["+v.size()+"];");});
			bldr.append("}");
			return "CacheStats [groupedResults=" + bldr.toString() + ", startTime=" + startTime + ", endTime=" + endTime
					+ ", elapsedRunTime=" + elapsedRunTime + "]";
		}
		
	}
	
	private static java.util.Date getNow(){
		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("America/Chicago"));
		return cal.getTime();
	}

	private static String formatDate(Date dt){
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss z");
		return df.format(dt);
	}

	private static long getElapsedTimeInSec(Date startDt, Date endDt){
		long diff = endDt.getTime() - startDt.getTime();
		return diff/1000;
	}

}
