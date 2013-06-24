/*
 * Copyright 2012 Honestyworks Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package jp.honestyworks.pbcache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.appengine.api.NamespaceManager;
import com.google.appengine.api.datastore.AsyncDatastoreService;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entities;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.FetchOptions;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.memcache.InvalidValueException;
import com.google.appengine.api.memcache.MemcacheService;
import com.google.appengine.api.memcache.MemcacheServiceFactory;

/**
 * Cache service.
 * 
 * @author miztaka
 *
 */
public class CacheService {
    
    public static final String NAMESPACE_PREFIX = "__ns:%s__";

	private static final Log logger = LogFactory.getLog(CacheService.class);

	private static final long LOCAL_CACHE_TTL = 5000;

	public static final int CHUNK_SIZE = 1000000;
	public static final String RESET_DATE_KIND = "CacheReset";
	public static final String RESET_DATE_PROP = "resetDate";
	public static final String KEY_RESET_DATE = "CacheResetDate:";
	public static final String KEY_RUNQUERY = "RunQuery:";
	
	// Local cache.
	private Map<String, Object> localCache;
	private long localCacheTime;
	private int localHits;
	private int cacheHits;
	private MemcacheService globalCache;
	private boolean localCacheUsed = true;

	/**
	 * Initialize cache service.
	 */
	public CacheService() {
	    globalCache = MemcacheServiceFactory.getMemcacheService();
        localCache = new HashMap<String, Object>();
        localCacheTime = System.currentTimeMillis();
	}
	
	/**
	 * Reset local cache according to TTL.
	 */
	public void resetLocalCache() {
		if (System.currentTimeMillis() - localCacheTime > LOCAL_CACHE_TTL) {
			localCache.clear(); 
	        localCacheTime = System.currentTimeMillis();
		}
	}

	/**
	 * Get caches of each key.
	 * 
	 * @param keys
	 * @return
	 */
	public Map getAll(Collection keys) {
		Map result = new HashMap();
		List memcacheKeys = new ArrayList();
		for (Object key : keys) {
		    String localKey = localKey((String)key);
			if (localCacheUsed && localCache.containsKey(localKey)) {
				result.put(key, localCache.get(localKey));
			}
			else {
				memcacheKeys.add(key);
			}
		}
		result.putAll(globalCache.getAll(memcacheKeys));
		return result;
	}

	/**
	 * Clear cache and reset date entity.
	 */
	public void clear() {
		localCache.clear();
		globalCache.clearAll();
		// clear resetDate of all namespaces
		DatastoreService ds = DatastoreServiceFactory.getDatastoreService();
		AsyncDatastoreService asyncDs = DatastoreServiceFactory.getAsyncDatastoreService();
		Query q = new Query(Entities.NAMESPACE_METADATA_KIND);
		List<String> results = new ArrayList<String>();
		for (Entity e : ds.prepare(q).asIterable()) {
		    results.add(Entities.getNamespaceFromNamespaceKey(e.getKey()));
		}
		String bak = NamespaceManager.get();
		try {
		    FetchOptions opt = FetchOptions.Builder.withOffset(0);
		    for(String ns: results) {
		        NamespaceManager.set(ns);
		        q = new Query(RESET_DATE_KIND).setKeysOnly();
		        List<Entity> entities = ds.prepare(q).asList(opt);
		        List<Key> keys = new ArrayList<Key>(entities.size());
		        for (Entity entity: entities) {
		            keys.add(entity.getKey());
		        }
		        asyncDs.delete(keys);
		    }
		} finally {
		    NamespaceManager.set(bak);
		}
		return;
	}

    /**
     * Put reset date for the key to cache and datastore.
     * in Memcache,Datastore
     */
	public void putResetDate(String keyname) {
	    
	    Date date = new Date();
        // datastore
        Key key = KeyFactory.createKey(RESET_DATE_KIND, keyname);
        Entity entity = new Entity(key);
        entity.setProperty(RESET_DATE_PROP, date);
        AsyncDatastoreService ds = DatastoreServiceFactory.getAsyncDatastoreService();
        ds.put(entity);
        // cache
        put(KEY_RESET_DATE + keyname, date);
        logger.debug("put reset date for : " + keyname);
        
        return;
	}
	
    /**
     * Get reset date for the key.
     * @param keyname
     * @return
     */
    public Date getResetDate(String keyname) {
        Date date = (Date)get(KEY_RESET_DATE + keyname);
        if (date != null) {
            return date;
        }
        // from datastore
        Key key = KeyFactory.createKey(RESET_DATE_KIND, keyname);
        Entity entity = null;
        try {
        	entity = DatastoreServiceFactory.getDatastoreService().get(key);
        } catch (EntityNotFoundException e) {
        }
        if (entity != null) {
            date = (Date)entity.getProperty(RESET_DATE_PROP);
            put(KEY_RESET_DATE + keyname, date);
            logger.debug("Get reset date from datastore: " + keyname);
            return date;
        }
        logger.debug("resetDate miss: " + keyname);
        return null;
    }
    
    /**
     * true if the key is contained.
     * @param arg0
     * @return
     */
	public boolean containsKey(Object arg0) {
		if (localCacheUsed && localCache.containsKey(localKey((String)arg0))) {
			return true;
		}
		return globalCache.contains(arg0);
	}

	/**
	 * Get cache by key.
	 * 
	 * @param key
	 * @return
	 */
	public Object get(Object key) {
		try {
		    String localKey = localKey((String)key);
			if (localCacheUsed && localCache.containsKey(localKey)) {
				localHits++;
				logger.debug("hit local cache: " + localKey);
				return localCache.get(localKey);
			}
			Object value = globalCache.get(key);
			if (value != null) {
				logger.debug("hit public cache: " + key);
				if (localCacheUsed) {
				    localCache.put((String)key, value);
				}
				cacheHits++;
				return value;
			}
			logger.debug("cache miss: " + key);
			return null;
		}
		catch (InvalidValueException e) {
			logger.error(e);
			return null;
		}
	}

	/**
	 * Put cache for the key.
	 * 
	 * @param key
	 * @param value
	 * @return
	 */
	public Object put(Object key, Object value) {
	    
	    if (localCacheUsed) {
	        String localKey = localKey((String)key);
	        localCache.put(localKey, value);
	    }
		try {
			globalCache.put(key, value);
			return value;
		}
		catch (Exception e) {
			logger.error(e);
			return value;
		}
	}

	/**
	 * Put all pair of key and value to cache.
	 * @param map
	 */
	public void putAll(Map map) {
	    
	    for (Object key: map.keySet()) {
	        if (localCacheUsed) {
	            String localKey = localKey((String)key);
	            localCache.put(localKey, map.get(key));
	        }
	    }
		//localCache.putAll(map);
		try {
			globalCache.putAll(map);
		}
		catch (Exception e) {
			logger.error(e.getMessage());
		}
	}

	/**
	 * Remove cache of the key.
	 * 
	 * @param key
	 * @return
	 */
	public Object remove(Object key) {
	    if (localCacheUsed) {
	        localCache.remove(localKey((String)key));
	    }
		try {
			return globalCache.delete(key);
		}
		catch (Exception e) {
			logger.error(e.getMessage());
			return null;
		}
	}

	public int getLocalHits() {
		return localHits;
	}

	public int getCacheHits() {
		return cacheHits;
	}

	/**
	 * Get large cached data.
	 * @param key
	 * @return
	 */
	public byte[] getBlob(String key) {
		String chunkList = (String)get(key);
		if (chunkList != null) {
			List<byte[]> data = new ArrayList<byte[]>();
			int size = 0;
			for (String chunkKey : chunkList.split(",")) {
				byte[] chunk = (byte[])get(chunkKey);
				if (chunk == null) {
					return null;
				}
				data.add(chunk);
				size += chunk.length;
			}
			return ChunkUtil.packChunks(data);
		}
		return null;
	}

	public static int CACHE_SIZE_LIMIT = 1000000;

	/**
	 * Put large cached data. 
	 * @param key
	 * @param data
	 */
	public void putBlob(String key, byte[] data) {
		List<String> chunkList = new ArrayList<String>();
		List<byte[]> chunks = ChunkUtil.makeChunks(data, CACHE_SIZE_LIMIT);
		int i = 0;
		for (byte[] chunk : chunks) {
			String chunkKey = key + String.valueOf(i);
			put(chunkKey, chunk);
			chunkList.add(chunkKey);
			i++;
		}
		put(key, StringUtils.join(chunkList, ","));
	}

	protected String localKey(String key) {
	    
        String namespace = NamespaceManager.get();
        String localKey = StringUtils.isEmpty(namespace) ? key :
            String.format(NAMESPACE_PREFIX, namespace) + key;
	    return localKey;
	}

	/**
	 * Set flag whether to use local cache.
	 * @param flag
	 */
    public void useLocalCache(boolean flag) {
        localCacheUsed = flag;
    }
    
    /**
     * CacheItemクラスとして登録されているキャッシュを取得します。
     * Blobにも対応しています。
     * 
     * @param key
     * @return
     */
    public CacheItem getCacheItem(String key) {
    	
		Object cachedData = get(key);
		if (cachedData != null) {
            if (cachedData instanceof CacheItem) {
                return (CacheItem)cachedData;
            } else {
                byte[] rawdata = getBlob(key);
                CacheItem item = (CacheItem)StreamUtil.toObject(rawdata);
                return item;
            }
		}
    	return null;
    }

}
