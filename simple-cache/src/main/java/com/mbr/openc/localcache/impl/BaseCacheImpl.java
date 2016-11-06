package com.mbr.openc.localcache.impl;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.mbr.openc.localcache.SimpleCache;

public abstract class BaseCacheImpl implements SimpleCache {

	protected Log log = LogFactory.getLog(getClass());
	private static boolean DEBUG = false;
	public static Properties config = null;

	public BaseCacheImpl(Properties cacheProps) throws Exception{
		DEBUG = log.isDebugEnabled();
		if(null == cacheProps || cacheProps.isEmpty())loadDefault();
		if(null != cacheProps && !cacheProps.isEmpty())this.config = cacheProps;
	}
	
	/**
	 * ABSTRACT 
	 */
	public abstract Object get(Object key) throws Exception;

	/**
	 * ABSTRACT 
	 */
	public abstract void put(Object key, Object value) throws Exception;

	/**
	 * ABSTRACT 
	 */
	public abstract boolean exists(Object key);

	/**
	 * ABSTRACT 
	 */
	public abstract int size();
	
	/**
	 * Load Default Properties from the SimpleCache.properties from the
	 * Classpath
	 */
	protected void loadDefault() throws Exception {

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
	protected void validateProps() throws Exception {
		log.info("Validating SimpleCache Properties ");

//		if (null == config.getProperty("cache.map.capacity") || config.getProperty("cache.map.capacity").equals("")) {
//			throw new Exception("Property[cache.map.capacity] is EMPTY. Cannot Proceed.");
//		}
//
//		if (!config.getProperty("cache.map.capacity").matches("[1-9][0-9]{1,3}")) {
//			throw new Exception(
//					"Property[cache.map.capacity] has NON-Numeric Characters OR exceeded the 4 digit limit. Anything Beyond 9999 is a Bad Design to use this Cache Strategy.");
//		}

		if (null == config.getProperty("cache.limit.size") || config.getProperty("cache.limit.size") .equals("")) {
			throw new Exception("Property[cache.limit.size] is EMPTY. Cannot Proceed.");
		}

		if (!config.getProperty("cache.limit.size").matches("[1-9][0-9]{0,6}")) {
			throw new Exception(
					"Property[cache.limit.size] has NON-Numeric Characters OR starting with ZERO or Exceeded the 6 digit limit. Anything Beyond 999999 is a Bad Design to use this Cache Strategy.");
		}
//		if (new Integer(config.getProperty("cache.limit.size")).intValue() >= new Integer(config.getProperty("cache.map.capacity")).intValue()){
//			throw new Exception(
//					"Property[cache.limit.size] CANNOT BE GREATER THAN or even Equal to [cache.map.capacity]. Your Map InitialCapcity Math needs Correction. Cannot Proceed");
//		}

		log.info("Completed Validating SimpleCache Properties ");
	}

}
