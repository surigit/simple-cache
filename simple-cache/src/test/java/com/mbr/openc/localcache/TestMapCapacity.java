package com.mbr.openc.localcache;

import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Before;
import org.junit.Test;

import com.mbr.openc.localcache.utils.SimpleCacheUtils;

public class TestMapCapacity {

	private static int initCapacity = 0;
	private static int cacheSize = 0;
	private static float loadFac = 0.90f;
	
	private static Log log = LogFactory.getLog(TestMapCapacity.class.getName());
	
	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void test() {
		int cacheSize = 125;
		System.out.println(SimpleCacheUtils.calcInitCapacity(cacheSize, true));
	}

	
	private void calcInitCapacity(int cacheSize, boolean adj4NoRehash){
		
		StringJoiner sj  = null; 
		double logOfCache = log2(cacheSize);
		int logCeil = (int)Math.ceil(logOfCache);
		this.initCapacity = (int)Math.pow(2,logCeil);
		this.cacheSize= cacheSize;
		int rehashRate =0;
		if(!adj4NoRehash){
			rehashRate = (int)(initCapacity*0.75f);
		}
		
		if(!adj4NoRehash){
			sj = new StringJoiner(";","[","]");
			sj.add("Cache Size Limit="+cacheSize);
			sj.add("InitCapacity="+this.initCapacity);
			sj.add("loadFac=0.75f");
			sj.add("RehashRate="+rehashRate);
			log.info(sj);
			if(rehashRate < cacheSize)
			log.info("There is a chance of atleast ONE time re-hashing once the cache size spills over - "+rehashRate);
			return;
		}
		sj = new StringJoiner(";","[","]");
		
		if(adj4NoRehash){
			if(rehashRate <= cacheSize){
				logCeil++;
				logCeil = (int)Math.ceil(logCeil);
				this.initCapacity = (int)Math.pow(2,logCeil);
				rehashRate = (int)(initCapacity*loadFac);
				this.cacheSize = (rehashRate-1);
			}
		}
		sj.add("Adj CacheSize Limit="+this.cacheSize);
		sj.add("Adj InitCapacity="+this.initCapacity);
		sj.add("Adj loadFac=0.90f");
		sj.add("Adj RehashRate="+rehashRate);
		sj.add("adjustFlag=true");
		log.info(sj);
	}

	public static double logb( double a, double b ){
	return Math.log(a) / Math.log(b);
	}	
	
	public static double log2( double a){
	return logb(a,2);
	}	

}
