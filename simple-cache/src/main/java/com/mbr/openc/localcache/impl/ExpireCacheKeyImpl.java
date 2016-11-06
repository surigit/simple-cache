package com.mbr.openc.localcache.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.mbr.openc.localcache.SimpleCacheKey;

public class ExpireCacheKeyImpl implements SimpleCacheKey {
	protected Log log = LogFactory.getLog(getClass());
	private boolean DEBUG = false;

	private Object cacheKey = null;
	private int expiryTime = 0;
	
	public ExpireCacheKeyImpl(Object cacheKey,int expiryTime) {
		super();
		if(expiryTime > 0 ) {
			this.expiryTime = expiryTime;
		}
		this.cacheKey = cacheKey;
		DEBUG= log.isDebugEnabled();
	}

	@Override
	public void setExpiryTimeInSecs(int timeInSecs) {
		if(timeInSecs > 0) {
			this.expiryTime = timeInSecs;
			return;
		}
		if(DEBUG)log.debug("Expiry Time for CacheKey ["+cacheKey+"] is <= ZERO. Ignored.");
	}

	@Override
	public void setCacheKey(Object cacheKey) {
		this.cacheKey = cacheKey;
	}

	
	@Override
	public int getExpiryTimeInSecs() {
		return this.expiryTime;
	}

	@Override
	public Object getCacheKey() {
		return this.cacheKey;
	}

	@Override
	public String toString() {
		return "ExpireCacheKeyImpl [cacheKey=" + cacheKey + ", expiryTime=" + expiryTime + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((cacheKey == null) ? 0 : cacheKey.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ExpireCacheKeyImpl other = (ExpireCacheKeyImpl) obj;
		if (cacheKey == null) {
			if (other.cacheKey != null)
				return false;
		} else if (!cacheKey.equals(other.cacheKey))
			return false;
		return true;
	}

	
}
