package com.mbr.openc.localcache.impl;

import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.mbr.openc.localcache.utils.SimpleCacheUtils;

public class StaticCache extends BaseCacheImpl {
	protected Log log = LogFactory.getLog(getClass());
	private static boolean DEBUG = false;
	private Properties config =null;
	private static int CACHE_LIMIT_SIZE=0;
	private static boolean NO_REHASH=false;
	private static CacheMap<Object, Object> concMap = null;

	
	public StaticCache(Properties cacheProps) throws Exception {
		super(cacheProps);
		this.config = BaseCacheImpl.config;
		DEBUG = log.isDebugEnabled();
		init();
	}

	/**
	 * Initialize 
	 */
	private void init() {

		log.info("StaticCache init() Invoked");
		CACHE_LIMIT_SIZE = Integer.parseInt(config.getProperty("cache.limit.size"));
		NO_REHASH = (config.getProperty("cache.with.norehash")!=null && "Y".equals(config.getProperty("cache.with.norehash")))?true:false;

		// Compute the InitCapacity here 
		ComputedCacheConfig cacheCfg = SimpleCacheUtils.calcInitCapacity(CACHE_LIMIT_SIZE, NO_REHASH);
		concMap = new CacheMap<Object, Object>(cacheCfg.getInitCapacity(), cacheCfg.getLoadFac(),cacheCfg.getCacheSize());
		log.info(cacheCfg);
		log.info("StaticCache init() Invocation Complete");
	}

	
	@Override
	public Object get(Object key) throws Exception {
		
		if(DEBUG) log.debug("StaticCache get Called ");
		// TODO: 1 : Check if the Object is in Cache. If so return it
		if (concMap.containsKey(key)) {
			if(DEBUG) log.debug("StaticCache returning data from Cache");
			return concMap.get(key);
		}

		return null;
	}

	@Override
	public void put(Object key, Object value) throws Exception {
		if(DEBUG) log.debug("StaticCache PUT Called ");
		if(null==key){
			log.error("Key Cannot be NULL. Please check the why the Key is NULL?");
			throw new Exception ("Key Cannot be NULL. Please check the why the Key is NULL?");
		}
		if(null==value){
			log.error("Value Cannot be NULL. NO NULLs allowed in Cache Object [ConcurrentHashMap]. Check for NULLS before calling the \"put\" method");
			throw new Exception ("Value Cannot be NULL. NO NULLs allowed in Cache Object [ConcurrentHashMap]. Check for NULLS before calling the \"set\" method");
		}
		concMap.put(key,value);
		if(DEBUG) log.debug("StaticCache PUT Complete ");

	}

	@Override
	public Object remove(Object key) {
		if(DEBUG) log.debug("StaticCache remove Called ");
		return concMap.remove(key);
	}

	@Override
	public boolean exists(Object key) {
		if(DEBUG) log.debug("StaticCache exists Called ");
		return concMap.keyExists(key);
	}

	@Override
	public int size() {
		if(DEBUG) log.debug("BaseCacheImpl cache size invoked");
		return concMap.size();
	}

}
