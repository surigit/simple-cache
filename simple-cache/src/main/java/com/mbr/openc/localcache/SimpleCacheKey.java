package com.mbr.openc.localcache;

public interface SimpleCacheKey {

	public void setExpiryTimeInSecs(int timeInSecs);
	
	public void setCacheKey(Object cacheKey);

	public int getExpiryTimeInSecs();
	
	public Object getCacheKey();
	
	public boolean equals(Object obj);

	public int hashCode();
	
}
