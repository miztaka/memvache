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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.apphosting.api.ApiProxy.ApiConfig;
import com.google.apphosting.api.ApiProxy.ApiProxyException;
import com.google.apphosting.api.ApiProxy.Delegate;
import com.google.apphosting.api.ApiProxy.Environment;
import com.google.apphosting.api.ApiProxy.LogRecord;
import com.google.apphosting.api.DatastorePb.NextRequest;
import com.google.apphosting.api.DatastorePb.PutRequest;
import com.google.apphosting.api.DatastorePb.DeleteRequest;
import com.google.apphosting.api.DatastorePb.Query;
import com.google.apphosting.api.DatastorePb.QueryResult;
import com.google.storage.onestore.v3.OnestoreEntity.EntityProto;
import com.google.storage.onestore.v3.OnestoreEntity.Path;
import com.google.storage.onestore.v3.OnestoreEntity.Reference;
import com.google.storage.onestore.v3.OnestoreEntity.Path.Element;

/**
 * Hook protocol buffer to cache query request.
 * 
 * @author miztaka
 *
 * @param <E>
 */
public class MemcacheProxyHook<E extends Environment> implements Delegate<E> {
	
	private static final java.lang.String API_DEADLINE_KEY = "com.google.apphosting.api.ApiProxy.api_deadline_key";
    
    private static final Log logger = LogFactory.getLog(MemcacheProxyHook.class);
    
    private Delegate<E> baseDelegate;

    public MemcacheProxyHook(Delegate<E> base) {
            this.baseDelegate = base;
    }

    public void flushLogs(E environment) {
        baseDelegate.flushLogs(environment);
    }

    public List<Thread> getRequestThreads(E environment) {
        return baseDelegate.getRequestThreads(environment);
    }

    public void log(E environment, LogRecord record) {
        baseDelegate.log(environment, record);
    }

    public Future<byte[]> makeAsyncCall(E environment, String packageName,
            String methodName, byte[] request, ApiConfig apiConfig) {
        
        logger.debug("makeAsyncCall: " + packageName + "#" + methodName);
        
        // タイムアウト設定
        logEnvironment(environment);
        logger.debug("apiConfig deadline: " + apiConfig.getDeadlineInSeconds());
        Object oldValue = environment.getAttributes().put(API_DEADLINE_KEY, new Double(0.1));
        try {
        	return baseDelegate.makeAsyncCall(environment, packageName, methodName, request, apiConfig);
        } finally {
        	if (oldValue == null) {
        		environment.getAttributes().remove(API_DEADLINE_KEY);
        	} else {
        		environment.getAttributes().put(API_DEADLINE_KEY, oldValue);
        	}
        }
    }

    public byte[] makeSyncCall(E environment, String packageName,
            String methodName, byte[] request) throws ApiProxyException {
        
        logger.debug("makeSyncCall: " + packageName + "#" + methodName);

        // タイムアウト設定
        logEnvironment(environment);
        
        return baseDelegate.makeSyncCall(environment, packageName, methodName, request);
    }
    
    private void logEnvironment(E environment) {
    	Object deadLine = environment.getAttributes().get(API_DEADLINE_KEY);
    	logger.debug("environment: " + environment);
    	logger.debug("deadline: " + deadLine);
    	logger.debug("remaining: " + environment.getRemainingMillis());
    }

}
