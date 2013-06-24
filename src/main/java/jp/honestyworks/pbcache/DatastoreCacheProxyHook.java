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
public class DatastoreCacheProxyHook<E extends Environment> implements Delegate<E> {
    
    private static final Log logger = LogFactory.getLog(DatastoreCacheProxyHook.class);
    private static final String[] IGNORE_KINDS = {
        CacheService.RESET_DATE_KIND,
        "_ah_SESSION"
    };
    
    private Delegate<E> baseDelegate;

    public DatastoreCacheProxyHook(Delegate<E> base) {
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
        
        if (methodName.equals("RunQuery")) {
            byte[] response = delegateRunQuery(environment, packageName, methodName, request);
            return new SyncFuture(response);
        }
        if (methodName.equals("Put")) {
            PutRequest putrequest = new PutRequest();
            putrequest.mergeFrom(request);
            removeCacheByEntity(putrequest.entitys());
        }
        if (methodName.equals("Delete")) {
            DeleteRequest pbrequest = new DeleteRequest(); 
            pbrequest.mergeFrom(request);
            removeCache(pbrequest.keys());
        }
        return baseDelegate.makeAsyncCall(environment, packageName, methodName, request, apiConfig);
    }

    public byte[] makeSyncCall(E environment, String packageName,
            String methodName, byte[] request) throws ApiProxyException {
        
        logger.debug("makeSyncCall: " + packageName + "#" + methodName);
        
        if (methodName.equals("RunQuery")) {
            return delegateRunQuery(environment, packageName, methodName, request);
        }
        if (methodName.equals("Put")) {
            PutRequest putrequest = new PutRequest();
            putrequest.mergeFrom(request);
            removeCacheByEntity(putrequest.entitys());
        }
        if (methodName.equals("Delete")) {
            DeleteRequest pbrequest = new DeleteRequest(); 
            pbrequest.mergeFrom(request);
            removeCache(pbrequest.keys());
        }
        return baseDelegate.makeSyncCall(environment, packageName, methodName, request);
    }
    
    /**
     * Remove query cache.
     * @param keys
     */
    private void removeCache(List<Reference> keys) {
        
        Map<String,Integer> buf = new HashMap<String,Integer>();
        for (Reference key: keys) {
        	String kind = getKindFromKey(key);
            if (! isIgnoredKind(kind)) {
                buf.put(kind, 1);
            }
        }
        for (String kind : buf.keySet()) {
            logger.debug("remove queryCache: " + kind);
            getQueryCache().removeQueries(kind);
        }
        return;
    }
    
    /**
     * @see removeCache
     * @param entitys
     */
    private void removeCacheByEntity(List<EntityProto> entitys) {
        
        List<Reference> keys = new ArrayList<Reference>(entitys.size());
        for (EntityProto each: entitys) {
            keys.add(each.getKey());
        }
        removeCache(keys);
        return;
    }
    
    /**
     * Is this kind ignored to cache.
     * @param kind
     * @return
     */
    private boolean isIgnoredKind(String kind) {
        
        if (kind == null) {
            return true;
        }
        if (kind.startsWith("__") && kind.endsWith("__")) {
            return true;
        }
        if (ArrayUtils.contains(IGNORE_KINDS, kind)) {
            return true;
        }
        return false;
    }

    /**
     * Process RunQuery.
     * 
     * @param environment
     * @param packageName
     * @param methodName
     * @param request
     * @return
     */
    private byte[] delegateRunQuery(E environment, String packageName,
            String methodName, byte[] request) {
        
        byte[] response = null;
        Query query = new Query();
        query.mergeFrom(request);
        String kind = query.getKind();
        logger.debug("kind: " + kind);
        if (isIgnoredKind(kind)) {
            return baseDelegate.makeSyncCall(environment, packageName, methodName, request);
        }
        
        // from cache
        response = getQueryCache().getQuery(kind, request);
        if (response != null) {
            return response;
        }
        
        
        // from datestore
        response = baseDelegate.makeSyncCall(environment, packageName, methodName, request);
        QueryResult result = new QueryResult();
        result.mergeFrom(response);
        logger.debug("QueryResult:hasMoreResults:" + result.hasMoreResults());
        logger.debug("QueryResult:isMoreResults:" + result.isMoreResults());
        
        // put to cache
        if (! result.isMoreResults()) {
        	getQueryCache().putQuery(kind, request, response);
        } else {
        	logger.warn("Because RunQuery has more results, it was not cached.");
        }
        return response;
    }
    
    /**
     * TODO こちらは未完成
     * ※RunQuery～全てのNextのキャッシュが残っていることが保証できないため、このやり方は破綻している
     * 
     * Nextを処理します
     * @param environment
     * @param packageName
     * @param methodName
     * @param request
     * @return
     */
    private byte[] delegateNext(E environment, String packageName,
            String methodName, byte[] request) {
        
        byte[] response = null;
        NextRequest query = new NextRequest();
        query.mergeFrom(request);
        logger.debug("Next: " + query.toFlatString(true));
        
        // キャッシュから取得
        /*
        response = getQueryCache().getQuery(kind, request);
        if (response != null) {
            return response;
        } 
        */       
        
        //return baseDelegate.makeSyncCall(environment, packageName, methodName, request);
        
        /*
        
        String kind = query.getKind();
        logger.debug("kind: " + kind);
        if (isIgnoredKind(kind)) {
            return baseDelegate.makeSyncCall(environment, packageName, methodName, request);
        }
        if (logger.isDebugEnabled()) {
            logger.debug("query: " + query.toString());
        }
        
        // キャッシュから取得
        response = getQueryCache().getQuery(kind, request);
        if (response != null) {
            return response;
        }
        */
        
        // Datestoreから取得
        response = baseDelegate.makeSyncCall(environment, packageName, methodName, request);
        QueryResult result = new QueryResult();
        result.mergeFrom(response);
        logger.debug("QueryResult:hasMoreResults:" + result.hasMoreResults());
        logger.debug("QueryResult:isMoreResults:" + result.isMoreResults());
        //logger.debug(result.toString());
        EntityProto proto = result.getResult(0);
        if (proto != null) {
            String kind = getKindFromKey(proto.getKey());
            logger.debug("Kind of Next response: " + kind);
        }
        
        // キャッシュに登録 (Nextがあるときは登録しない)
        //if (! result.isMoreResults()) {
            //getQueryCache().putQuery(kind, request, response);
        //} else {
            //logger.warn("Because RunQuery has more results, don't cache it.");
        //}
        return response;
    }    
    
    /**
     * キーからKindを取得
     * @param key
     * @return
     */
    private String getKindFromKey(Reference key) {
        Path p = key.getPath();
        logger.debug("key: " + p);
        int size = p.elementSize();
        Element target = p.getElement(size-1);
        String kind = target.getType();
        logger.debug("kind: " + target.getType());
        return kind;
    }

    private QueryCache getQueryCache() {
    	return CacheContext.getInstance().getQueryCache();
    }

    static class SyncFuture implements Future<byte[]> {
        final byte[] result;
        final Throwable cause;

        SyncFuture(byte[] result) {
            this(result, null);
        }

        SyncFuture(Throwable cause) {
            this(null, cause);
        }

        private SyncFuture(byte[] result, Throwable cause) {
            this.result = result;
            this.cause = cause;
        }

        public byte[] get() throws InterruptedException, ExecutionException {
            if (result != null) return result;
            throw new ExecutionException(cause);
        }

        public byte[] get(long l, TimeUnit timeunit) throws InterruptedException,
                ExecutionException, TimeoutException {
            return get();
        }

        public boolean cancel(boolean flag) {
            return false;
        }

        public boolean isCancelled() {
            return false;
        }

        public boolean isDone() {
            return true;
        }

    }

}
