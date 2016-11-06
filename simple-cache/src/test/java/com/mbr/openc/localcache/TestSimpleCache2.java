package com.mbr.openc.localcache;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Properties;
import java.util.Random;
import java.util.StringJoiner;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import com.mbr.openc.localcache.SimpleCache;
import com.mbr.openc.localcache.SimpleCacheFactory;
import com.mbr.openc.localcache.SimpleCacheWithExpiry;
import com.mbr.openc.localcache.impl.ExpireCacheKeyImpl;
import com.mbr.openc.localcache.impl.SampleDatasourceImpl;


public class TestSimpleCache2 {

	List<String> symbol = new ArrayList();
	SimpleCache myCache = null;
	SimpleCacheWithExpiry expiryCache = null;

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
		symbol.add("LMLP");
		symbol.add("RAVN");
		symbol.add("ALG");
		symbol.add("PODD");
		symbol.add("AMSC");
		symbol.add("TK");
		symbol.add("TTC");
		symbol.add("KEM");
		symbol.add("GEOS");
		symbol.add("NCS");
		symbol.add("GE");
		symbol.add("BA");
		symbol.add("DE");
		symbol.add("CAT");
		symbol.add("DAL");
		symbol.add("PACB");
		symbol.add("TROV");
		symbol.add("SRPT");
		symbol.add("ARRY");
		symbol.add("MNOV");
		symbol.add("RGEN");
		symbol.add("OCLR");
		symbol.add("CYTX");
		symbol.add("ALKS");
		symbol.add("VRX");
		symbol.add("ANAC");
		symbol.add("BAX");
		symbol.add("JNJ");
		symbol.add("VRX");
		symbol.add("GILD");
		symbol.add("BCOM");
		symbol.add("LMOS");
		symbol.add("CTL");
		symbol.add("CBB");
		symbol.add("NQ");
		symbol.add("TEO");
		symbol.add("VG");
		symbol.add("TLK");
		symbol.add("GNCMA");
		symbol.add("T");
		symbol.add("VZ");
		symbol.add("CMCSA");
		symbol.add("CTL");
		symbol.add("VOD");
	}

	//@Test
	public void testStaticCache2() throws Exception {
		/**
		 * STATIC CACHE USAGE ----------------------------------------- ` *
		 * Required Properties ----------------------------------------- NOTE:
		 * The following are the defaults the Cache will load
		 * cache.refresh.interval=3600 (time in seconds)
		 * cache.refresh.interval=3600
		 * cache.limit.size=100
		 * cache.with.norehash=Y
		 * Important: The CaChe is set up with a 90% Load Factor IF no-rehash Option is Y and will never
		 * grow out of the Initial Capacity. If the Flag is NOT Y - then 75% load factor is taken. 
		 * 
		 * *
		 */

		// For Static Cache - ONLY ONE Properties are required.
		Properties props = new Properties();
		props.setProperty("cache.limit.size", "100");
		
		//Note: if you pass NULL as props to the below factory. The props will be loaded default from the Jar
		SimpleCache myCache = SimpleCacheFactory.getStaticCacheInstance(props);

		if (myCache.exists("JPM")) {
			// SO PUT INTO IT
			myCache.put("JPM", "JPMorgan Chase.....");
		}

		if (!myCache.exists("JPM")) {
			myCache.put("JPM", "JPMorgan Chase.....");
		}

		System.out.println(myCache.get("JPM"));

	}

	//@Test
	public void testReadThru_RefreshBehind_Cache() throws Exception {
		/**
		 * Read-Thru:Refresh-Behind CACHE USAGE
		 * ----------------------------------------- ` * Required Properties
		 * ----------------------------------------- NOTE: The following are the
		 * defaults the Cache will load cache.map.capacity=128
		 * cache.limit.size=100 cache.refresh.interval=3600 (time in seconds)
		 * 
		 * Important: The Ccahe is set up with a 90% Load Factor and will never
		 * grow out of the Initial Capacity Please Refer Excel Map Math file to
		 * calculate the map Initial Capacity for your Target Capacity
		 * (cache.limit.size)
		 * 
		 * *
		 */
		Properties props = new Properties();
		props.setProperty("cache.limit.size", "5");
		props.setProperty("cache.with.norehash", "Y");
		props.setProperty("cache.refresh.interval", "10"); // 10 seconds

		myCache = SimpleCacheFactory.getReadThruWriteBehindCache(new SampleDatasourceImpl(), props);

		ExecutorService exeServ = Executors.newCachedThreadPool();

		exeServ.submit(new QueryJPM_VLO());
		exeServ.submit(new QueryJPM_VLO());

		exeServ.submit(new QueryRandom());
		exeServ.submit(new QueryRandom());

		exeServ.shutdown();

		exeServ.awaitTermination(10, TimeUnit.MINUTES);

		Thread.sleep(240 * 1000);
	}

	@Test
	public void testReadThruRefreshAndExpireBehind_Cache() throws Exception {
		/**
		 * Read-Thru:Refresh-Behind:Expire-Behind CACHE USAGE
		 * ----------------------------------------- ` * Required Properties
		 * ----------------------------------------- NOTE: The following are the
		 * defaults the Cache will load
		 * cache.with.norehash=Y
		 * cache.limit.size=100 
		 * cache.refresh.interval=3600 (time in seconds)
		 * 
		 * 
		 * *
		 */
		Properties props = new Properties();
		//props.setProperty("cache.with.norehash", "Y");
		props.setProperty("cache.limit.size", "10");
		props.setProperty("cache.refresh.interval", "10"); // 60seconds

		expiryCache = SimpleCacheFactory.getReadThruWriteAndExpireBehind(new SampleDatasourceImpl(), props);

		// expiryCache.get(new ExpireCacheKeyImpl("WFC", 2));
		// Thread.sleep(5000);
		// System.out.println("CacheSize ="+expiryCache.size());

		ExecutorService exeServ = Executors.newCachedThreadPool();

		exeServ.submit(new PumpRandom());
		exeServ.submit(new PumpRandom_1());
		//exeServ.submit(new ReadTopDown());
		//exeServ.submit(new ReadBottomUp() );

		exeServ.shutdown();

		exeServ.awaitTermination(5, TimeUnit.MINUTES);

		Thread.sleep(240 * 1000);
	}

	private class QueryJPM_VLO implements Callable<String> {

		@Override
		public String call() throws Exception {

			for (int i = 0; i < 100; i++) {

				System.out.println("Query result for [JPM] is [" + myCache.get("JPM") + "]");
				Thread.sleep(5000);
				System.out.println("Query result for [WFC] is [" + myCache.get("WFC") + "]");
				Thread.sleep(1000);
			}

			return "QY";
		}

	}

	private class QueryRandom implements Callable<String> {

		Random r = new Random();

		@Override
		public String call() throws Exception {

			for (int i = 0; i < 100; i++) {

				String s = symbol.get(r.nextInt(24));
				System.out.println("Random Query result for [" + s + "] is [" + myCache.get(s) + "]");
				Thread.sleep(2000);
			}

			return "QY";
		}

	}

	private class PumpRandom implements Callable<String> {

		Random r = new Random();

		@Override
		public String call() throws Exception {

			for (int i = 0; i < 100; i++) {
				StringJoiner joiner = new StringJoiner(",", "[", "]");
				int rand = r.nextInt(33);
				String s = symbol.get(rand);
				joiner.add(s);
				joiner.add(new Integer(rand).toString());
				System.out.println("Pumping 0 Ticker " + joiner.toString());
				//expiryCache.get(new ExpireCacheKeyImpl(s, rand));
				expiryCache.get(new ExpireCacheKeyImpl(s, 3));
				Thread.sleep(3000);
			}

			return "QY";
		}

	}

	private class PumpRandom_1 implements Callable<String> {

		Random r = new Random();

		@Override
		public String call() throws Exception {

			for (int i = 0; i < 100; i++) {
				int rand = r.nextInt(67);
				if (rand <= 33)
					continue;
				String s = symbol.get(rand);
				StringJoiner joiner = new StringJoiner(",", "[", "]");
				joiner.add(s);
				joiner.add(new Integer(rand).toString());
				System.out.println("Pumping 1 Ticker " + joiner.toString());
				//expiryCache.get(new ExpireCacheKeyImpl(s, rand));
				expiryCache.get(new ExpireCacheKeyImpl(s, 4));
				Thread.sleep(4000);
			}

			return "QY";
		}

	}

	private class ReadTopDown implements Callable<String> {

		Random r = new Random();

		@Override
		public String call() throws Exception {

			for (int i = 0; i < 2; i++) {

				symbol.forEach(a -> {
					int rand = r.nextInt(4);
					try {
						StringJoiner joiner = new StringJoiner(",", "[", "]");
						joiner.add(a);
						joiner.add(new Integer(rand).toString());
						System.out.println("ReadTopDown Ticker " + joiner.toString());
						expiryCache.get(new ExpireCacheKeyImpl(a, rand));
						Thread.sleep(1000);
					} catch (Exception e) {
						System.err.println("Error:ReadTopDown key[" + a + "] - " + e.getLocalizedMessage());
					}
				});

			}

			return "QY";
		}
	}

	private class ReadBottomUp implements Callable<String> {

		@Override
		public String call() throws Exception {

			for (int i = 0; i < 2; i++) {

				ListIterator<String> itr = symbol.listIterator(symbol.size());
				while (itr.hasPrevious()) {
					String sym = itr.previous();
					StringJoiner joiner = new StringJoiner(",", "[", "]");
					joiner.add(sym);
					joiner.add("0");
					System.out.println("ReadBottomUp Ticker " + joiner.toString());
					expiryCache.get(new ExpireCacheKeyImpl(sym, 0));
					Thread.sleep(1000);
				}
			}

			return "QY";
		}
	}

}
