package com.mbr.openc.localcache.impl;

public class ComputedCacheConfig {

	private int initCapacity;
	private float loadFac;
	private int cacheSize;
	public ComputedCacheConfig(int initCapacity, float loadFac, int cacheSize) {
		super();
		this.initCapacity = initCapacity;
		this.loadFac = loadFac;
		this.cacheSize = cacheSize;
	}
	public int getInitCapacity() {
		return initCapacity;
	}
	public float getLoadFac() {
		return loadFac;
	}
	public int getCacheSize() {
		return cacheSize;
	}
	@Override
	public String toString() {
		return "ComputedCacheConfig [initCapacity=" + initCapacity + ", loadFac=" + loadFac + ", cacheSize=" + cacheSize
				+ "]";
	}

	
}
