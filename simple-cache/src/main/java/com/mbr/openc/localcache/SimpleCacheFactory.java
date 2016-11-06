package com.mbr.openc.localcache;

import java.util.Properties;

import com.mbr.openc.localcache.impl.ReadThruRefreshAndExpireBehind;
import com.mbr.openc.localcache.impl.ReadThruRefreshBehind;
import com.mbr.openc.localcache.impl.SimpleCacheImpl;
import com.mbr.openc.localcache.impl.StaticCache;

public final class SimpleCacheFactory {

	private static SimpleCache[] scArr = new SimpleCache[4];
	private static SimpleCacheWithExpiry simplExpiry = null;;
	
	@Deprecated
	public static SimpleCache getSimpleStaticCacheInstance(Properties cacheProperties) throws Exception{
	
		if(null == scArr[0]){
			scArr[0] = new SimpleCacheImpl(cacheProperties);
		}
		return scArr[0];
	}

	@Deprecated
	public static SimpleCache getAutoRefreshSimpleCacheInstance(SimpleCacheDatasource dataSrc,Properties cacheProperties,boolean autoRefresh) throws Exception{
		
		if(null == scArr[1]){
			scArr[1] = new SimpleCacheImpl(dataSrc,cacheProperties,autoRefresh);
		}
		
		return scArr[1];
	}
	
	public static SimpleCache getStaticCacheInstance(Properties cacheProperties) throws Exception{
		
		if(null == scArr[2]){
			scArr[2] = new StaticCache(cacheProperties);
		}
		
		return scArr[2];
	}

	public static SimpleCache getReadThruWriteBehindCache(SimpleCacheDatasource dataSrc,Properties cacheProperties) throws Exception{
		
		if(null == scArr[3]){
			scArr[3] = new ReadThruRefreshBehind(dataSrc,cacheProperties);
		}
		
		return scArr[3];
	}
	
	public static SimpleCacheWithExpiry getReadThruWriteAndExpireBehind(SimpleCacheDatasource dataSrc,Properties cacheProperties) throws Exception{
		
		if(null == simplExpiry){
			simplExpiry = new ReadThruRefreshAndExpireBehind(dataSrc,cacheProperties);
		}
		return simplExpiry;
	}

}
