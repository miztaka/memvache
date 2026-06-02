package net.vvakame.memvache;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.EntityTranslatorPublic;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.Transaction;
import com.google.appengine.api.memcache.MemcacheService;
import com.google.apphosting.api.DatastorePb;
import com.google.storage.onestore.v3.OnestoreEntity.EntityProto;
import java.util.Map;
import org.junit.Test;
import org.slim3.datastore.Datastore;
import org.slim3.memcache.Memcache;
import org.slim3.tester.ControllerTestCase;

/**
 * {@link GetPutCacheStrategy} のテストケース。
 * @author vvakame
 */
public class GetPutCacheStrategyTest extends ControllerTestCase {

  MemvacheDelegate memvacheDelegate;

  RpcCounterDelegate countDelegate;

  /**
   * テストケース。
   * @author vvakame
   * @throws EntityNotFoundException
   */
  @Test
  public void put_notAllocatedId() throws EntityNotFoundException {
    DatastoreService datastore = DatastoreServiceFactory.getDatastoreService();
    Entity entity = new Entity("hoge");
    Key key = datastore.put(entity);
    datastore.get(key);
  }

  /**
   * テストケース。
   * @author vvakame
   */
  @Test
  public void put_withTx_withCommit() {
    Key key = Datastore.createKey("hoge", 1);
    Transaction tx = Datastore.beginTransaction();
    Datastore.put(new Entity(key));

    assertThat("Tx下なので0", Memcache.statistics().getItemCount(), is(0L));

    tx.commit();

    assertThat("1つput", MemvacheDelegate.getMemcache().get(key), notNullValue());
  }

  /**
   * テストケース。
   * @author vvakame
   */
  @Test
  public void put_with2Tx_withCommit() {
    Key key1 = Datastore.createKey("hoge", 1);
    Transaction tx1 = Datastore.beginTransaction();
    Datastore.put(new Entity(key1));

    assertThat("Tx下なので0", Memcache.statistics().getItemCount(), is(0L));
    tx1.commit();
    assertThat("1つ目put", MemvacheDelegate.getMemcache().get(key1), notNullValue());

    Key key2 = Datastore.createKey("hoge", 2);
    Transaction tx2 = Datastore.beginTransaction();
    Datastore.put(new Entity(key2));

    tx2.commit();
    assertThat("2つ目put", MemvacheDelegate.getMemcache().get(key2), notNullValue());
  }

  /**
   * テストケース。
   * @author vvakame
   */
  @Test
  public void put_withTx_withRollback() {
    Transaction tx = Datastore.beginTransaction();
    Datastore.put(new Entity("hoge", 1));

    assertThat("Tx下なので0", Memcache.statistics().getItemCount(), is(0L));

    tx.rollback();

    assertThat("なかったことに", Memcache.statistics().getItemCount(), is(0L));
  }

  /**
   * テストケース。
   * @author vvakame
   */
  @Test
  public void get_existsAllCache() {
    Key key;
    {
      Entity entity = new Entity("hoge", 1);
      entity.setProperty("v1", 1);
      MemcacheService memcache = MemvacheDelegate.getMemcache();
      key = entity.getKey();
      EntityProto proto = EntityTranslatorPublic.convertToPb(entity);
      DatastorePb.GetResponse.Entity en = new DatastorePb.GetResponse.Entity();
      en.setEntity(proto);
      en.setKey(proto.getKey());
      memcache.put(key, en);
    }

    Map<String, Integer> countMap = countDelegate.countMap;
    countMap.clear();

    Datastore.get(key);

    assertThat("あった", countMap.get("memcache@Get"), is(1));
    assertThat("あった", countMap.get("datastore_v3@Get"), is(0));
    assertThat("GetしてないのでSetなし", countMap.get("memcache@Set"), is(0));
  }

  /**
   * テストケース。
   * @author vvakame
   */
  @Test
  public void get_existsDefectCache() {
    Key key1;
    {
      Entity entity = new Entity("hoge", 1);
      entity.setProperty("v1", 1);
      MemcacheService memcache = MemvacheDelegate.getMemcache();
      key1 = entity.getKey();
      EntityProto entityProto = EntityTranslatorPublic.convertToPb(entity);
      com.google.apphosting.api.DatastorePb.GetResponse.Entity en =
          new DatastorePb.GetResponse.Entity();
      en.setEntity(entityProto);
      memcache.put(key1, en);
    }

    Key key2;
    {
      Entity entity = new Entity("hoge", 2);
      entity.setProperty("v1", 1);
      key2 = entity.getKey();
      Datastore.put(entity);
      MemvacheDelegate.getMemcache().delete(key2);
    }

    Map<String, Integer> countMap = countDelegate.countMap;
    countMap.clear();

    Datastore.get(key1, key2);

    assertThat("あった", countMap.get("memcache@Get"), is(1));
    assertThat("1つない", countMap.get("datastore_v3@Get"), is(1));
    assertThat("1つ新規", countMap.get("memcache@Set"), is(1));
  }

  /**
   * テストケース。
   * @author vvakame
   */
  @Test
  public void delete() {
    Entity entity = new Entity("hoge", 1);
    Key key = Datastore.put(entity);
    MemvacheDelegate.getMemcache().put(key, entity);

    assertThat(Memcache.statistics().getItemCount(), is(1L));

    Datastore.delete(key);

    assertThat(Memcache.statistics().getItemCount(), is(0L));
  }

  /**
   * テストケース。
   * @author vvakame
   */
  @Test
  public void run_RPCs() {
    Key key = Datastore.createKey("hoge", 20);
    Entity entity = new Entity(key);
    Datastore.put(entity);
    Datastore.get(key);
    Datastore.delete(key);
    Datastore.getOrNull(key);
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();

    // かならず RpcCounterDelegate が最初
    countDelegate = RpcCounterDelegate.install();

    memvacheDelegate =
        MemvacheDelegate.install(
            StrategyBuilder.newBuilder()
                .addStrategy(MemvacheDelegate.DATASTORE_V3, GetPutCacheStrategy.class)
                .buid());
    // memvacheDelegate.strategies.get().clear();
    // memvacheDelegate.strategies.get().add(new GetPutCacheStrategy());
  }

  @Override
  public void tearDown() throws Exception {
    memvacheDelegate.uninstall();
    countDelegate.uninstall();

    super.tearDown();
  }
}
