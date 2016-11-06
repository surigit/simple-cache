package com.mbr.openc.localcache;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import com.mbr.openc.localcache.SimpleCache;
import com.mbr.openc.localcache.SimpleCacheFactory;
import com.mbr.openc.localcache.impl.SampleDatasourceImpl;

public class TestSimpleCache {

	List<String> symbol = new ArrayList();
	SimpleCache myCache = null;
	
	@Before
	public void setUp() throws Exception {

		symbol.add("JPM");
		symbol.add("WFC");
		symbol.add("KEY");
		symbol.add("PSX");
		symbol.add("MPC");
		symbol.add("VLO");
		symbol.add("TSO");
		symbol.add("HFC");
		symbol.add("DK");
		symbol.add("NTI");
		symbol.add("PBF");
		symbol.add("FITB");
		symbol.add("BNPQY");
		symbol.add("BEN");
		symbol.add("PSX");
		symbol.add("MPC");
		symbol.add("VLO");
		symbol.add("TSO");
		symbol.add("HFC");
		symbol.add("DK");
		symbol.add("NTI");
		symbol.add("PBF");
		symbol.add("CLMT");
		symbol.add("WNR");
		symbol.add("ALDW");

	}

	
	//@Test
	public void testStaticCache() throws Exception{
		/**
		 * STATIC CACHE USAGE
		 * ----------------------------------------- 
`		 * Required Properties 
		 * ----------------------------------------- 
		 * NOTE: The following are the defaults the Cache will load 
		 * cache.map.capacity=128
		 * cache.limit.size=100
		 * cache.refresh.interval=3600 (time in seconds)
		 * 
		 * Important: The Ccahe is set up with a 90% Load Factor and will never grow out of the Initial Capacity 
		 * Please Refer Excel Map Math file calculate the map Initial Capacity for your Target Capacity (cache.limit.size)
		 * 
		 *  		 *  
		 */

		// For Static Cache - ONLY two Properties are required. 
		Properties props = new Properties();
		props.setProperty("cache.map.capacity", "128");
		props.setProperty("cache.limit.size", "100");		
		SimpleCache myCache = SimpleCacheFactory.getSimpleStaticCacheInstance(props);
		
		if(myCache.exists("JPM")){
			// SO PUT INTO IT 
			myCache.put("JPM","JPMorgan Chase.....");
		}
		
		if(!myCache.exists("JPM")){
			// Handle to query JPM from your Datasource 
			
			// TODO :: Query Datasource 
			// queryDataSource();............

			myCache.put("JPM","JPMorgan Chase.....");
		
		}

		System.out.println(myCache.get("JPM"));
		
		
	}

	@Test
	public void testAutoCache() throws Exception{
		/**
		 * AUTO CACHE USAGE
		 * ----------------------------------------- 
`		 * Required Properties 
		 * ----------------------------------------- 
		 * NOTE: The following are the defaults the Cache will load 
		 * cache.map.capacity=128
		 * cache.limit.size=100
		 * cache.refresh.interval=3600 (time in seconds)
		 * 
		 * Important: The Ccahe is set up with a 90% Load Factor and will never grow out of the Initial Capacity 
		 * Please Refer Excel Map Math file calculate the map Initial Capacity for your Target Capacity (cache.limit.size)
		 * 
		 *  		 * 
		 */
		Properties props = new Properties();
		props.setProperty("cache.map.capacity", "128");
		props.setProperty("cache.limit.size", "100");
		props.setProperty("cache.refresh.interval", "10"); // 10 seconds
		
		myCache = SimpleCacheFactory.getAutoRefreshSimpleCacheInstance(new SampleDatasourceImpl(), props, true);

		ExecutorService exeServ = Executors.newCachedThreadPool();
		
		exeServ.submit(new QueryJPM_VLO());
		exeServ.submit(new QueryJPM_VLO());
		

		exeServ.submit(new QueryRandom());
		exeServ.submit(new QueryRandom());

		exeServ.shutdown();
		
		exeServ.awaitTermination(10, TimeUnit.MINUTES);
		
		Thread.sleep(240*1000);
	}

	
	private class QueryJPM_VLO implements Callable<String>{

		@Override
		public String call() throws Exception {
			
			for(int i =0;i<100;i++){

				System.out.println("Query result for [JPM] is ["+myCache.get("JPM")+"]");
				Thread.sleep(5000);
				System.out.println("Query result for [WFC] is ["+myCache.get("WFC")+"]");
				Thread.sleep(1000);
			}
			
			return "QY";
		}
		
	}
	

	private class QueryRandom implements Callable<String>{

		Random r = new Random();

		@Override
		public String call() throws Exception {
			
			for(int i =0;i<100;i++){

				String s = symbol.get(r.nextInt(24));
				System.out.println("Random Query result for ["+s+"] is ["+myCache.get(s)+"]");
				Thread.sleep(2000);
			}
			
			return "QY";
		}
		
	}

}
