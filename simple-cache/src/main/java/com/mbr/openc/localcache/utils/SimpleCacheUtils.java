package com.mbr.openc.localcache.utils;

import java.util.StringJoiner;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.mbr.openc.localcache.impl.ComputedCacheConfig;

public class SimpleCacheUtils {

	private static Log log = LogFactory.getLog(SimpleCacheUtils.class.getName());
	
	public static ComputedCacheConfig calcInitCapacity(int cacheSize, boolean adj4NoRehash){
		
		StringJoiner sj  = null; 
		double logOfCache = log2(cacheSize);
		int logCeil = (int)Math.ceil(logOfCache);
		int initCapacity = (int)Math.pow(2,logCeil);
		int rehashRate =0;
		if(!adj4NoRehash){
			rehashRate = (int)(initCapacity*0.75f);
		}
		
		if(!adj4NoRehash){
			sj = new StringJoiner(";","[","]");
			sj.add("Cache Size Limit="+cacheSize);
			sj.add("InitCapacity="+initCapacity);
			sj.add("loadFac=0.75f");
			sj.add("RehashRate="+rehashRate);
			log.info(sj);
			if(rehashRate < cacheSize)
			log.info("There is a chance of atleast ONE time re-hashing once the cache size spills over - "+rehashRate);
			return new ComputedCacheConfig(initCapacity,0.75f,cacheSize);
		}
		sj = new StringJoiner(";","[","]");
		
		if(adj4NoRehash){
			if(rehashRate <= cacheSize){
				logCeil++;
				logCeil = (int)Math.ceil(logCeil);
				initCapacity = (int)Math.pow(2,logCeil);
				rehashRate = (int)(initCapacity*0.90f);
				cacheSize = (rehashRate-1);
			}
		}
		sj.add("Adj CacheSize Limit="+cacheSize);
		sj.add("Adj InitCapacity="+initCapacity);
		sj.add("Adj loadFac=0.90f");
		sj.add("Adj RehashRate="+rehashRate);
		sj.add("adjustFlag=true");
		log.info(sj);
		return new ComputedCacheConfig(initCapacity,0.90f,cacheSize);
	}

	public static double logb( double a, double b ){
	return Math.log(a) / Math.log(b);
	}	
	
	public static double log2( double a){
	return logb(a,2);
	}	

}
