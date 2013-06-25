package net.vvakame.memvache;

import java.io.IOException;
import java.util.logging.Logger;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;

/**
 * {@link MemvacheDelegate} を適用するための {@link Filter}。
 * @author vvakame
 */
public class MemvacheFilter implements Filter {

	static final Logger logger = Logger.getLogger(MemvacheFilter.class.getSimpleName());
	
	MemvacheDelegate delegate;

	@Override
	public void init(FilterConfig filterConfig) {
		
		RpcVisitor.debug = false;
		delegate = MemvacheDelegate.install(
			StrategyBuilder.newBuilder()
				.addStrategy(MemvacheDelegate.DATASTORE_V3, QueryKeysOnlyStrategy.class)
				.addStrategy(MemvacheDelegate.DATASTORE_V3, GetPutCacheStrategy.class)
				.buid()
		);
	}

	@Override
	public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
			throws IOException, ServletException {
		chain.doFilter(request, response);
		
		// ストラテジーをクリア
		delegate.requestFinished();
	}

	protected void preProcess(MemvacheDelegate delegate) {
	}

	void doThrow(Throwable th) throws IOException, ServletException {
		if (th instanceof ServletException) {
			throw (ServletException) th;
		}
		if (th instanceof IOException) {
			throw (IOException) th;
		}
		throw new ServletException(th);
	}

	@Override
	public void destroy() {
	}
	
}
