package net.vvakame.memvache;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.slim3.util.StringUtil;

import com.google.appengine.api.memcache.MemcacheService;
import com.google.apphosting.api.ApiProxy;
import com.google.apphosting.api.ApiProxy.ApiConfig;
import com.google.apphosting.api.ApiProxy.ApiProxyException;
import com.google.apphosting.api.ApiProxy.Delegate;
import com.google.apphosting.api.ApiProxy.Environment;
import com.google.apphosting.api.ApiProxy.LogRecord;

/**
 * Memvache のコアとなる {@link Delegate}。<br>
 * 1リクエスト中ではStrategyのインスタンス再生成は行わず使いまわす。
 * @author vvakame
 */
public class MemvacheDelegate implements ApiProxy.Delegate<Environment> {
	
	private static final long RPC_TIMEOUT = 3000L;

	static final Logger logger = Logger.getLogger(MemvacheDelegate.class.getName());
	
	public static final String DATASTORE_V3 = "datastore_v3"; 

	final ApiProxy.Delegate<Environment> parent;
	
	/**
	 * 各サービスごとに適用するストラテジーの設定
	 */
	final private Map<String,List<Class<? extends Strategy>>> strategyConfig;

	/**
	 * スレッドローカルなストラテジーのインスタンス
	 */
	ThreadLocal<Map<String, List<Strategy>>> localStrategies = new ThreadLocal<Map<String, List<Strategy>>>();

	/**
	 * {@link MemvacheDelegate}を{@link ApiProxy}に設定する。
	 * <p>
	 * 現在{@link ApiProxy}に設定されている
	 * {@link com.google.apphosting.api.ApiProxy.Delegate}が
	 * {@link MemvacheDelegateV2}だった場合は何もしない。
	 * </p>
	 * 
	 * @return 新たに作成した{@link MemvacheDelegate}か、 既に適用済みだった場合は元々設定されていた
	 *         {@link MemvacheDelegate}
	 */
	public static MemvacheDelegate install(Map<String,List<Class<? extends Strategy>>> config) {
		logger.fine("MemvacheDelegate install called.");
		@SuppressWarnings("unchecked")
		Delegate<Environment> originalDelegate = ApiProxy.getDelegate();
		if (originalDelegate instanceof MemvacheDelegate == false) {
			MemvacheDelegate newDelegate = new MemvacheDelegate(originalDelegate, config);
			ApiProxy.setDelegate(newDelegate);
			return newDelegate;
		} else {
			logger.warning("original Delegate is MemvacheDelegate");
			MemvacheDelegate delegate = (MemvacheDelegate) originalDelegate;
			return delegate;
		}
	}

	/**
	 * {@link MemvacheDelegate}を{@link ApiProxy}からはずす。
	 * 
	 * @param originalDelegate
	 *            元々設定されていた{@link com.google.apphosting.api.ApiProxy.Delegate}.
	 *            {@link MemvacheDelegate#getParent()}を使用すると良い。
	 */
	public static void uninstall(Delegate<Environment> originalDelegate) {
		ApiProxy.setDelegate(originalDelegate);
	}

	/**
	 * {@link MemvacheDelegate}を{@link ApiProxy}からはずす。
	 */
	public void uninstall() {
		ApiProxy.setDelegate(parent);
	}
	
	/**
	 * ストラテジーのインスタンスをクリアする。
	 */
	public void initStrategies() {
		logger.fine("localStrategiesを初期化します。" + Thread.currentThread().getId());
		localStrategies.set(null);
	}
	
	/**
	 * スレッドローカルなStrategyのListを取得します。
	 * @return
	 */
	private List<Strategy> getLocalStrategies(String service) {
		
		Map<String, List<Strategy>> strategies = localStrategies.get();
		if (strategies == null) {
			logger.fine("localStrategiesを構築します。 " + Thread.currentThread().getId());
			// 全サービスのストラテジーをインスタンス化
			strategies = new HashMap<String, List<Strategy>>();
			for (String key: strategyConfig.keySet()) {
				List<Strategy> list = new ArrayList<Strategy>();
				strategies.put(key, list);
				for (Class<? extends Strategy> clazz: strategyConfig.get(key)) {
					try {
						list.add(clazz.newInstance());
					} catch (InstantiationException e) {
						throw new RuntimeException(e);
					} catch (IllegalAccessException e) {
						throw new RuntimeException(e);
					}
				}
			}
			localStrategies.set(strategies);
		}
		return strategies.get(service);
	}

	@Override
	public Future<byte[]> makeAsyncCall(Environment env, final String service, final String method,
			final byte[] requestBytes, ApiConfig config) {
		return processAsyncCall(env, service, method, requestBytes, config, 0);
	}

	Future<byte[]> processAsyncCall(Environment env, final String service, final String method,
			final byte[] requestBytes, ApiConfig config, int depth) {

		List<Strategy> strategies = getLocalStrategies(service);
		
		// 適用すべき戦略がなかったら実際のRPCを行う
		if (strategies == null || strategies.size() == depth) {
			return getParent().makeAsyncCall(env, service, method, requestBytes, config);
		}

		final Strategy strategy = strategies.get(depth);

		// responseが生成されていたらそっちを結果として返す
		final Pair<byte[], byte[]> pair = strategy.preProcess(service, method, requestBytes);
		if (pair != null && pair.response != null) {
			return createFuture(pair.response);
		}

		// 次の戦略を適用する。もしリクエストが改変されてたらそっちを渡す。
		Future<byte[]> response;
		if (pair != null && pair.request != null) {
			response = processAsyncCall(env, service, method, pair.request, config, depth + 1);
		} else {
			response = processAsyncCall(env, service, method, requestBytes, config, depth + 1);
		}

		// responseが改変されてたらそっちを結果として返す
		return new SniffFuture<byte[]>(response) {

			@Override
			public byte[] processDate(byte[] data) {
				byte[] modified;
				if (pair != null && pair.request != null) {
					modified = strategy.postProcess(service, method, pair.request, data);
				} else {
					modified = strategy.postProcess(service, method, requestBytes, data);
				}

				if (modified != null) {
					return modified;
				} else {
					return data;
				}
			}
		};
	}

	@Override
	public byte[] makeSyncCall(Environment env, String service, String method, byte[] requestBytes)
			throws ApiProxyException {

		return processSyncCall(env, service, method, requestBytes, 0);
	}

	byte[] processSyncCall(Environment env, String service, String method, byte[] requestBytes,
			int depth) {
		
		List<Strategy> strategies = getLocalStrategies(service);

		// 適用すべき戦略がなかったら実際のRPCを行う
		if (strategies == null || strategies.size() == depth) {
			return getParent().makeSyncCall(env, service, method, requestBytes);
		}

		Strategy strategy = strategies.get(depth);

		// responseが生成されていたらそっちを結果として返す
		Pair<byte[], byte[]> pair = strategy.preProcess(service, method, requestBytes);
		if (pair != null && pair.response != null) {
			return pair.response;
		}

		// 次の戦略を適用する。もしリクエストが改変されてたらそっちを渡す。
		byte[] response;
		byte[] processedRequest = (pair != null && pair.request != null) ?
				pair.request : requestBytes;
		response = processSyncCall(env, service, method, processedRequest, depth + 1);

		// responseが改変されてたらそっちを結果として返す
		byte[] modified = strategy.postProcess(service, method, processedRequest, response);
		if (modified != null) {
			return modified;
		} else {
			return response;
		}
	}

	/**
	 * Namespaceがセット済みの {@link MemcacheService} を取得する。
	 * ※NamespaceがセットされていないMemcacheServiceWrapperを返すように変更。(タイムアウト設定のため)
	 * 
	 * @return {@link MemcacheService}
	 * @author vvakame
	 */
	public static MemcacheService getMemcache() {
		//return MemcacheServiceFactory.getMemcacheService("memvache");
		String memcacheTimeout = System.getProperty("memcache.timeout");
		if (! StringUtil.isEmpty(memcacheTimeout)) {
			return new MemcacheServiceWrapper(Long.parseLong(memcacheTimeout));
		} else {
			return new MemcacheServiceWrapper(RPC_TIMEOUT);
		}
	}

	/**
	 * 指定したデータを処理結果として返す {@link Future} を作成し返す。
	 * @param data 処理結果データ
	 * @return {@link Future}
	 * @author vvakame
	 */
	Future<byte[]> createFuture(final byte[] data) {
		return new Future<byte[]>() {

			@Override
			public boolean cancel(boolean mayInterruptIfRunning) {
				return false;
			}

			@Override
			public byte[] get() {
				return data;
			}

			@Override
			public byte[] get(long timeout, TimeUnit unit) {
				return data;
			}

			@Override
			public boolean isCancelled() {
				return false;
			}

			@Override
			public boolean isDone() {
				return true;
			}
		};
	}

	/**
	 * the constructor.
	 * 
	 * @param delegate
	 * @category constructor
	 */
	MemvacheDelegate(Delegate<Environment> delegate, Map<String,List<Class<? extends Strategy>>> config) {
		this.parent = delegate;
		this.strategyConfig = config;
	}

	@Override
	public void log(Environment env, LogRecord logRecord) {
		getParent().log(env, logRecord);
	}

	@Override
	public void flushLogs(Environment env) {
		getParent().flushLogs(env);
	}

	@Override
	public List<Thread> getRequestThreads(Environment env) {
		return getParent().getRequestThreads(env);
	}

	/**
	 * @return the parent
	 * @category accessor
	 */
	public ApiProxy.Delegate<Environment> getParent() {
		return parent;
	}
}
