package com.mbr.openc.localcache.impl;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class CacheMap<K,V> extends ConcurrentHashMap<Object, Object> {
	protected Log log = LogFactory.getLog(getClass());

	private AccessOrderMap<Object, Object> expMap = null;
	private Lock ACCESS_ORDER_LOCK = new ReentrantLock(true);
	private int cSize = 0;
	
	
	public CacheMap(int initialCapacity, float loadFactor, int concurrencyLevel, int cacheSize) {
		super(initialCapacity, loadFactor, concurrencyLevel);
		cSize = cacheSize;
		expMap = new AccessOrderMap(initialCapacity, loadFactor, true,cacheSize);
	}

	public CacheMap(int initialCapacity, float loadFactor, int cacheSize) {
		super(initialCapacity, loadFactor);
		cSize = cacheSize;
		expMap = new AccessOrderMap(initialCapacity, loadFactor, true,cacheSize);
	}

	public CacheMap(int initialCapacity,int cacheSize) {
		super(initialCapacity);
		cSize = cacheSize;
		expMap = new AccessOrderMap(initialCapacity, 0.90f, true,cacheSize);
	}

	// override methods here for PUT ONLY
	@Override
	public Object put(Object key, Object value) {
		Object prevObj = super.put(key, value);
		try {
			// TODO : Acquire fair lock here
			ACCESS_ORDER_LOCK.lock();
			// TODO : Put the object in the LinkedHashMap
			// Maintain only the Keys here, Value is DUMMY. Hence "A"
			expMap.put(key, "A");
			// TODO : Check if an Object is evicted here
			if(expMap.getRemovedKey() == null) log.error("No Key removed here");
			if (null != expMap.getRemovedKey()) {
				super.remove(expMap.getRemovedKey());
				log.error("Key removed here from ConcMap");
			}
			log.debug(this.size()+"#C#"+cSize);
			log.debug(expMap.size()+"#E#"+cSize);
			
		} catch (Exception e) {
			log.error("In CacheMap Put - while handling Expiry Map" + e);
		} finally {
			// release the Lock here 
			ACCESS_ORDER_LOCK.unlock();
		}
		return prevObj;
	}

	/**
	 * While get- also invoke the get on the LinkedHashMap - which governs the access order 
	 */
	@Override
	public Object get(Object key) {
		expMap.get(key);
		return super.get(key);
	}

	@Override
	public Object remove(Object key) {
		Object remKey = null;
		if(super.get(key)!=null){
			remKey = super.remove(key);
			try {
				ACCESS_ORDER_LOCK.lock();
				Object lruO = expMap.remove(key);
				if(null==lruO)log.debug("Removal Key["+key+"] NOT in expiryMap");
			} catch (Exception e) {
				log.error("Error while removing Key ["+key+"] from ExpiryMap.Suppressing Error",e);
			} finally {
				// release the Lock here 
				ACCESS_ORDER_LOCK.unlock();
			}

		}
		return remKey;
	}

	public boolean keyExists(Object key){
		return (super.get(key)==null)?false:true;
	}
	
}
