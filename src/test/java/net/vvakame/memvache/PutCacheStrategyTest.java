package net.vvakame.memvache;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.apphosting.api.DatastorePb;
import org.junit.Test;
import org.slim3.tester.ControllerTestCase;

/**
 * {@link PutCacheStrategy} のテストケース。
 * @author vvakame
 */
public class PutCacheStrategyTest extends ControllerTestCase {

  MemvacheDelegate memvacheDelegate;

  RpcCounterDelegate countDelegate;

  /**
   * テストケース。
   * @author vvakame
   */
  @Test
  public void put_notAllocatedId_setsAllocatedKeyInCachedEntityProto() {
    Entity entity = new Entity("hoge");
    entity.setProperty("v1", 1);

    DatastoreService datastore = DatastoreServiceFactory.getDatastoreService();
    Key key = datastore.put(entity);

    Object cached = MemvacheDelegate.getMemcache().get(key);
    assertThat(cached, instanceOf(DatastorePb.GetResponse.Entity.class));

    DatastorePb.GetResponse.Entity cachedEntity = (DatastorePb.GetResponse.Entity) cached;
    assertThat(PbKeyUtil.toKey(cachedEntity.getKey()), is(key));
    assertThat(PbKeyUtil.toKey(cachedEntity.getEntity().getKey()), is(key));
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();

    // かならず RpcCounterDelegate が最初
    countDelegate = RpcCounterDelegate.install();

    memvacheDelegate =
        MemvacheDelegate.install(
            StrategyBuilder.newBuilder()
                .addStrategy(MemvacheDelegate.DATASTORE_V3, PutCacheStrategy.class)
                .buid());
  }

  @Override
  public void tearDown() throws Exception {
    memvacheDelegate.uninstall();
    countDelegate.uninstall();

    super.tearDown();
  }
}
