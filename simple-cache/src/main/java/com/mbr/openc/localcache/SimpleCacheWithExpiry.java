package com.mbr.openc.localcache;

public interface SimpleCacheWithExpiry extends SimpleCache {

	public Object get(SimpleCacheKey key) throws Exception;
	
	public Object remove(SimpleCacheKey key) throws Exception;

}
