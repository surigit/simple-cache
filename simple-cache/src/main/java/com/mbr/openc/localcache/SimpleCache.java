package com.mbr.openc.localcache;

public interface SimpleCache {

	public Object get(Object key) throws Exception;
	
	public void put(Object key, Object value) throws Exception ;

	public Object remove(Object key);

	public boolean exists(Object key);

	public int size();

}
