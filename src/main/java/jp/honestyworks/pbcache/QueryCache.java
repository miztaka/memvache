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

import java.io.Serializable;
import java.util.Date;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Datastore RunQuery cache.
 * 
 * @author miztaka
 *
 */
@SuppressWarnings("serial")
public class QueryCache implements Serializable {

	protected static final Log logger = LogFactory.getLog(
			QueryCache.class);

	public QueryCache() {
	}

	private CacheService getCache() {
		return CacheContext.getInstance().getCacheService();
	}

	private String getQueryKey(byte[] request) {
	    String b64request = Base64.encodeBase64String(request);
	    return CacheService.KEY_RUNQUERY + b64request;
	}

	private String getClassResetdateKey(String kind) {
		return CacheService.KEY_RUNQUERY + kind;
	}
	
	private Date getClassResetDate(String  kind) {
		return getCache().getResetDate(getClassResetdateKey(kind));
	}
	
	/**
	 * Get query cache.
	 * 
	 * @param kind
	 * @param request
	 * @return
	 */
	public byte[] getQuery(String kind, byte[] request) {
		try {
		    CacheItem item = null;
		    String key = getQueryKey(request);
			Object cachedData = getCache().get(key);
			if (cachedData != null) {
	            if (cachedData instanceof CacheItem) {
	                item = (CacheItem)cachedData;
	            } else {
	                byte[] rawdata = getCache().getBlob(key);
	                item = (CacheItem)StreamUtil.toObject(rawdata);
	            }
                Date classResetDate = getClassResetDate(kind);
                logger.debug("class reset date: " + classResetDate);
                if (classResetDate == null
                        || item.getTimestamp().after(classResetDate)) {
                    logger.debug("query cache hit: " + kind);
                    return getCachedQueryResult(item);
                }
			}
		}
		catch (Exception e) {
			logger.error(ExceptionUtils.getStackTrace(e));
		}
		logger.debug("query cache miss: " + kind);
		return null;
	}

	private byte[] getCachedQueryResult(CacheItem item) {
		return (byte[])item.getData();
	}
	
	/**
	 * Put query result to cache.
	 * @param kind
	 * @param request
	 * @param response
	 */
    public void putQuery(String kind, byte[] request, byte[] response) {
        String key = getQueryKey(request);
        logger.debug("put query cache: " + kind + " " + key);
        CacheItem item = new CacheItem(response);
        if (response.length > CacheService.CHUNK_SIZE) {
            getCache().putBlob(key, StreamUtil.toBytes(item));
        } else {
            getCache().put(key, item);
        }
        return;
    }

	public void removeQueries(String kind) {
		getCache().putResetDate(getClassResetdateKey(kind));
	}

}
