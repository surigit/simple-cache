package com.mbr.openc.localcache.impl;

import java.util.LinkedHashMap;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class AccessOrderMap<K, V> extends LinkedHashMap<Object, Object> {
	protected Log log = LogFactory.getLog(getClass());

	private Object removedKey = null;
	private static boolean DEBUG = false;
	private int SIZE_LIMIT = 10;

	public AccessOrderMap() {
		super();
		// TODO Auto-generated constructor stub
	}

	public AccessOrderMap(int initialCapacity, float loadFactor, boolean accessOrder, int sizeLimit) {
		super(initialCapacity, loadFactor, accessOrder);
		// TODO Auto-generated constructor stub
		SIZE_LIMIT = sizeLimit;
		DEBUG = log.isDebugEnabled();
	}

	@Override
	protected boolean removeEldestEntry(Entry<Object, Object> eldest) {
		if (super.size()>SIZE_LIMIT) {
			this.removedKey = eldest.getKey();
			if(DEBUG)log.debug("Evicting LRU Key[" + removedKey + "]");
			return true;
		}
		removedKey = null;
		return false;
	}

	public Object getRemovedKey() {
		return removedKey;
	}

}
