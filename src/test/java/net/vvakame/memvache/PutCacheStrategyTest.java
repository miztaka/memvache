package net.vvakame.memvache;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.Transaction;
import com.google.apphosting.api.DatastorePb;
import org.junit.Test;
import org.slim3.datastore.Datastore;
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
  public void put_withCompleteKey_cachesEntityProto() {
    DatastoreService datastore = DatastoreServiceFactory.getDatastoreService();
    Entity entity = new Entity("hoge", 1);
    entity.setProperty("v1", 1);
    Key key = Datastore.put(entity);

    Object cached = MemvacheDelegate.getMemcache().get(key);
    assertThat(cached, instanceOf(DatastorePb.GetResponse.Entity.class));

    DatastorePb.GetResponse.Entity cachedEntity = (DatastorePb.GetResponse.Entity) cached;
    assertThat(PbKeyUtil.toKey(cachedEntity.getEntity().getKey()), is(key));
  }

  /**
   * テストケース。
   * @author vvakame
   */
  @Test
  public void get_cachesReturnedEntity() throws Exception {
    DatastoreService datastore = DatastoreServiceFactory.getDatastoreService();
    Entity entity = new Entity("hoge", 1);
    entity.setProperty("v1", 1);
    Key key = Datastore.put(entity);

    MemvacheDelegate.getMemcache().delete(key);
    assertThat(MemvacheDelegate.getMemcache().get(key), nullValue());

    datastore.get(key);

    Object cached = MemvacheDelegate.getMemcache().get(key);
    assertThat(cached, instanceOf(DatastorePb.GetResponse.Entity.class));
    DatastorePb.GetResponse.Entity cachedEntity = (DatastorePb.GetResponse.Entity) cached;
    assertThat(PbKeyUtil.toKey(cachedEntity.getEntity().getKey()), is(key));
  }

  /**
   * テストケース。
   * @author vvakame
   */
  @Test
  public void put_withTx_withRollback() {
    DatastoreService datastore = DatastoreServiceFactory.getDatastoreService();
    Transaction tx = datastore.beginTransaction();
    Entity entity = new Entity("hoge", 1);
    Key key = datastore.put(tx, entity);

    assertThat("Tx下なので未反映", MemvacheDelegate.getMemcache().get(key), nullValue());

    tx.rollback();

    assertThat("rollback後も未反映", MemvacheDelegate.getMemcache().get(key), nullValue());
  }

  /**
   * テストケース。
   * @author vvakame
   */
  @Test
  public void delete_removesCachedEntity() {
    DatastoreService datastore = DatastoreServiceFactory.getDatastoreService();
    Entity entity = new Entity("hoge", 1);
    Key key = Datastore.put(entity);
    MemvacheDelegate.getMemcache().put(key, "cached");

    assertThat(MemvacheDelegate.getMemcache().get(key), notNullValue());

    datastore.delete(key);

    assertThat(MemvacheDelegate.getMemcache().get(key), nullValue());
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
