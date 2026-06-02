package net.vvakame.memvache;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import com.google.appengine.api.datastore.Entity;
import jp.honestyworks.pbcache.CacheContext;
import jp.honestyworks.pbcache.QueryCache;
import org.junit.Test;
import org.slim3.datastore.Datastore;
import org.slim3.tester.ControllerTestCase;

/**
 * {@link ResetDateStrategy} のテストケース。
 * @author vvakame
 */
public class ResetDateStrategyTest extends ControllerTestCase {

  MemvacheDelegate memvacheDelegate;

  RpcCounterDelegate countDelegate;

  /**
   * テストケース。
   * @author vvakame
   */
  @Test
  public void put_invalidatesQueryCacheForSameKind() {
    QueryCache queryCache = CacheContext.getInstance().getQueryCache();
    byte[] request = "request".getBytes();
    byte[] response = "response".getBytes();
    queryCache.putQuery("hoge", request, response);

    assertThat(queryCache.getQuery("hoge", request), is(response));

    Datastore.put(new Entity("hoge", 1));

    assertThat(queryCache.getQuery("hoge", request), nullValue());
  }

  /**
   * テストケース。
   * @author vvakame
   */
  @Test
  public void delete_invalidatesQueryCacheForSameKind() {
    Entity entity = new Entity("hoge", 1);
    Datastore.put(entity);

    QueryCache queryCache = CacheContext.getInstance().getQueryCache();
    byte[] request = "request".getBytes();
    byte[] response = "response".getBytes();
    queryCache.putQuery("hoge", request, response);

    assertThat(queryCache.getQuery("hoge", request), is(response));

    Datastore.delete(entity.getKey());

    assertThat(queryCache.getQuery("hoge", request), nullValue());
  }

  /**
   * テストケース。
   * @author vvakame
   */
  @Test
  public void ignoreKind_doesNotInvalidateQueryCache() {
    ResetDateStrategy.Settings settings = ResetDateStrategy.Settings.getInstance();
    settings.ignoreKinds.add("hoge");

    try {
      QueryCache queryCache = CacheContext.getInstance().getQueryCache();
      byte[] request = "request".getBytes();
      byte[] response = "response".getBytes();
      queryCache.putQuery("hoge", request, response);

      Datastore.put(new Entity("hoge", 1));

      assertThat(queryCache.getQuery("hoge", request), is(response));
    } finally {
      settings.ignoreKinds.remove("hoge");
    }
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();

    // かならず RpcCounterDelegate が最初
    countDelegate = RpcCounterDelegate.install();

    memvacheDelegate =
        MemvacheDelegate.install(
            StrategyBuilder.newBuilder()
                .addStrategy(MemvacheDelegate.DATASTORE_V3, ResetDateStrategy.class)
                .buid());
  }

  @Override
  public void tearDown() throws Exception {
    memvacheDelegate.uninstall();
    countDelegate.uninstall();

    super.tearDown();
  }
}
