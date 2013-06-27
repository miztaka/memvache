package net.vvakame.memvache;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

import com.google.appengine.api.memcache.AsyncMemcacheService;
import com.google.appengine.api.memcache.ErrorHandler;
import com.google.appengine.api.memcache.Expiration;
import com.google.appengine.api.memcache.MemcacheService;
import com.google.appengine.api.memcache.MemcacheServiceFactory;
import com.google.appengine.api.memcache.Stats;

/**
 * タイムアウトをかけるために、
 * AsyncMemcacheServiceをMemcacheServiceのインターフェースでWrapする
 * 
 * @author miztaka
 *
 */
public class MemcacheServiceWrapper implements MemcacheService {
	
	private static final Logger logger = Logger.getLogger(MemcacheServiceWrapper.class.getName());
	
	private long timeoutMilliSec;
	private AsyncMemcacheService asyncService;
	
	public MemcacheServiceWrapper(long timeoutMilliSec) {
		this.timeoutMilliSec = timeoutMilliSec;
		asyncService = MemcacheServiceFactory.getAsyncMemcacheService();
	}
	

	@Override
	public ErrorHandler getErrorHandler() {
		return asyncService.getErrorHandler();
	}

	@Override
	public String getNamespace() {
		return asyncService.getNamespace();
	}

	@Override
	public void setErrorHandler(ErrorHandler arg0) {
		asyncService.setErrorHandler(arg0);
	}

	@Override
	public void clearAll() {
		asyncService.clearAll();
	}

	@Override
	public boolean contains(Object arg0) {
		return getQuietly(asyncService.contains(arg0));
	}

	@Override
	public boolean delete(Object arg0) {
		return getQuietly(asyncService.delete(arg0));
	}

	@Override
	public boolean delete(Object arg0, long arg1) {
		return getQuietly(asyncService.delete(arg0, arg1));
	}

	@Override
	public <T> Set<T> deleteAll(Collection<T> arg0) {
		return getQuietly(asyncService.deleteAll(arg0));
	}

	@Override
	public <T> Set<T> deleteAll(Collection<T> arg0, long arg1) {
		return getQuietly(asyncService.deleteAll(arg0, arg1));
	}

	@Override
	public Object get(Object arg0) {
		return getQuietly(asyncService.get(arg0));
	}

	@Override
	public <T> Map<T, Object> getAll(Collection<T> arg0) {
		return getQuietly(asyncService.getAll(arg0));
	}

	@Override
	public IdentifiableValue getIdentifiable(Object arg0) {
		return getQuietly(asyncService.getIdentifiable(arg0));
	}

	@Override
	public <T> Map<T, IdentifiableValue> getIdentifiables(Collection<T> arg0) {
		return getQuietly(asyncService.getIdentifiables(arg0));
	}

	@Override
	public Stats getStatistics() {
		return getQuietly(asyncService.getStatistics());
	}

	@Override
	public Long increment(Object arg0, long arg1) {
		return getQuietly(asyncService.increment(arg0, arg1));
	}

	@Override
	public Long increment(Object arg0, long arg1, Long arg2) {
		return getQuietly(asyncService.increment(arg0, arg1, arg2));
	}

	@Override
	public <T> Map<T, Long> incrementAll(Map<T, Long> arg0) {
		return getQuietly(asyncService.incrementAll(arg0));
	}

	@Override
	public <T> Map<T, Long> incrementAll(Collection<T> arg0, long arg1) {
		return getQuietly(asyncService.incrementAll(arg0, arg1));
	}

	@Override
	public <T> Map<T, Long> incrementAll(Map<T, Long> arg0, Long arg1) {
		return getQuietly(asyncService.incrementAll(arg0, arg1));
	}

	@Override
	public <T> Map<T, Long> incrementAll(Collection<T> arg0, long arg1,
			Long arg2) {
		return getQuietly(asyncService.incrementAll(arg0, arg1, arg2));
	}

	@Override
	public void put(Object arg0, Object arg1) {
		asyncService.put(arg0, arg1);
	}

	@Override
	public void put(Object arg0, Object arg1, Expiration arg2) {
		asyncService.put(arg0, arg1, arg2);
	}

	@Override
	public boolean put(Object arg0, Object arg1, Expiration arg2, SetPolicy arg3) {
		return getQuietly(asyncService.put(arg0, arg1, arg2, arg3), 0);
	}

	@Override
	public void putAll(Map<?, ?> arg0) {
		asyncService.putAll(arg0);
	}

	@Override
	public void putAll(Map<?, ?> arg0, Expiration arg1) {
		asyncService.putAll(arg0, arg1);
	}

	@Override
	public <T> Set<T> putAll(Map<T, ?> arg0, Expiration arg1, SetPolicy arg2) {
		return getQuietly(asyncService.putAll(arg0, arg1, arg2), 0);
	}

	@Override
	public <T> Set<T> putIfUntouched(Map<T, CasValues> arg0) {
		return getQuietly(asyncService.putIfUntouched(arg0), 0);
	}

	@Override
	public <T> Set<T> putIfUntouched(Map<T, CasValues> arg0, Expiration arg1) {
		return getQuietly(asyncService.putIfUntouched(arg0, arg1), 0);
	}

	@Override
	public boolean putIfUntouched(Object arg0, IdentifiableValue arg1,
			Object arg2) {
		return getQuietly(asyncService.putIfUntouched(arg0, arg1, arg2), 0);
	}

	@Override
	public boolean putIfUntouched(Object arg0, IdentifiableValue arg1,
			Object arg2, Expiration arg3) {
		return getQuietly(asyncService.putIfUntouched(arg0, arg1, arg2, arg3), 0);
	}

	@Override
	public void setNamespace(String arg0) {
		throw new UnsupportedOperationException();
	}
	
    /**
     * Gets a value from the {@link Future} without throwing an exception.
     * 
     * @param <T>
     *            the value type
     * 
     * @param future
     *            the future
     * @return a value
     */
    private <T> T getQuietly(Future<T> future) {
    	return getQuietly(future, timeoutMilliSec);
    }

    /**
     * Gets a value from the {@link Future} without throwing an exception.
     * 
     * @param <T>
     *            the value type
     * 
     * @param future
     *            the future
     * @return a value
     */
    private <T> T getQuietly(Future<T> future, long millisec) {
        if (future == null) {
            throw new NullPointerException(
                "The future parameter must not be null.");
        }
        try {
            return millisec > 0L ? 
           		future.get(millisec, TimeUnit.MILLISECONDS) : future.get();
        } catch (ExecutionException e) {
        	logger.severe(e.getMessage());
        } catch (InterruptedException e) {
        	logger.severe(e.getMessage());
        } catch (TimeoutException e) {
			logger.severe(e.getMessage());
		}
        return null;
    }
    
}
