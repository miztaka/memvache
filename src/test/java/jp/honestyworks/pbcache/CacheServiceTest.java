package jp.honestyworks.pbcache;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import com.google.appengine.api.NamespaceManager;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.slim3.tester.AppEngineTestCase;

/**
 * {@link CacheService} のテストケース。
 */
public class CacheServiceTest extends AppEngineTestCase {

  @Test
  public void putAndGet_localCacheReturnsIndependentSnapshots() {
    CacheService cache = new CacheService();
    List<String> original = new ArrayList<String>(Collections.singletonList("original"));

    cache.put("snapshot", original);
    original.add("changed after put");

    List<String> first = (List<String>) cache.get("snapshot");
    assertThat(first, is(Collections.singletonList("original")));
    assertNotSame(original, first);

    first.add("changed after get");
    List<String> second = (List<String>) cache.get("snapshot");

    assertThat(second, is(Collections.singletonList("original")));
    assertNotSame(first, second);
    assertThat(cache.getLocalHits(), is(2));
    assertThat(cache.getCacheHits(), is(0));
  }

  @Test
  public void putAllAndGetAll_localCacheReturnsIndependentSnapshots() {
    CacheService cache = new CacheService();
    List<String> original = new ArrayList<String>(Collections.singletonList("original"));
    Map<String, Object> values = new HashMap<String, Object>();
    values.put("snapshot-all", original);

    cache.putAll(values);
    original.add("changed after putAll");

    Map firstResult = cache.getAll(Collections.singleton("snapshot-all"));
    List<String> first = (List<String>) firstResult.get("snapshot-all");
    assertThat(first, is(Collections.singletonList("original")));

    first.add("changed after getAll");
    Map secondResult = cache.getAll(Collections.singleton("snapshot-all"));
    List<String> second = (List<String>) secondResult.get("snapshot-all");

    assertThat(second, is(Collections.singletonList("original")));
    assertNotSame(first, second);
    assertThat(cache.getLocalHits(), is(0));
    assertThat(cache.getCacheHits(), is(0));
  }

  @Test
  public void get_globalResultStoredAsIndependentLocalSnapshot() {
    CacheService globalWriter = new CacheService();
    globalWriter.useLocalCache(false);
    globalWriter.put(
        "global-snapshot", new ArrayList<String>(Collections.singletonList("original")));

    CacheService cache = new CacheService();
    List<String> first = (List<String>) cache.get("global-snapshot");
    first.add("changed after global get");

    List<String> second = (List<String>) cache.get("global-snapshot");

    assertThat(second, is(Collections.singletonList("original")));
    assertNotSame(first, second);
    assertThat(cache.getCacheHits(), is(1));
    assertThat(cache.getLocalHits(), is(1));
  }

  @Test
  public void localCache_isSeparatedByNamespace() {
    CacheService cache = new CacheService();
    try {
      NamespaceManager.set("first");
      cache.put("namespace-key", new ArrayList<String>(Collections.singletonList("first-value")));

      NamespaceManager.set("second");
      cache.put("namespace-key", new ArrayList<String>(Collections.singletonList("second-value")));

      NamespaceManager.set("first");
      assertThat(
          (List<String>) cache.get("namespace-key"), is(Collections.singletonList("first-value")));

      NamespaceManager.set("second");
      assertThat(
          (List<String>) cache.get("namespace-key"), is(Collections.singletonList("second-value")));
    } finally {
      NamespaceManager.set(null);
    }
  }

  @Test
  public void put_nullValueDoesNotRemainInLocalCache() throws Exception {
    CacheService cache = new CacheService();
    cache.put("nullable", "existing");

    cache.put("nullable", null);

    assertFalse(localCache(cache).containsKey(cache.localKey("nullable")));
  }

  @Test
  public void put_unserializableValueDoesNotRemainInLocalCache() {
    CacheService cache = new CacheService();

    cache.put("unserializable", new Object());

    assertNull(cache.get("unserializable"));
    assertThat(cache.getLocalHits(), is(0));
    assertThat(cache.getCacheHits(), is(0));
  }

  @Test
  public void get_corruptLocalSnapshotFallsBackToGlobalCache() throws Exception {
    CacheService cache = new CacheService();
    cache.useLocalCache(false);
    cache.put("corrupt", new ArrayList<String>(Collections.singletonList("global-value")));
    cache.useLocalCache(true);

    localCache(cache).put(cache.localKey("corrupt"), new byte[] {1, 2, 3});

    assertThat((List<String>) cache.get("corrupt"), is(Collections.singletonList("global-value")));
    assertThat(cache.getLocalHits(), is(0));
    assertThat(cache.getCacheHits(), is(1));
  }

  @Test
  public void resetLocalCache_preservesTtlBehavior() throws Exception {
    CacheService cache = new CacheService();
    List<String> original = new ArrayList<String>(Collections.singletonList("original"));
    cache.put("ttl", original);
    original.add("changed");

    Field localCacheTime = CacheService.class.getDeclaredField("localCacheTime");
    localCacheTime.setAccessible(true);
    localCacheTime.setLong(cache, 0L);
    cache.resetLocalCache();

    assertThat((List<String>) cache.get("ttl"), is(Collections.singletonList("original")));
    assertThat(cache.getLocalHits(), is(0));
    assertThat(cache.getCacheHits(), is(1));
  }

  @Test
  public void putAndGet_largeByteArray_usesChunks() {
    int originalLimit = CacheService.CACHE_SIZE_LIMIT;
    CacheService.CACHE_SIZE_LIMIT = 4;
    try {
      CacheService cache = new CacheService();
      cache.useLocalCache(false);
      byte[] value = new byte[] {1, 2, 3, 4, 5, 6, 7};

      cache.put("large-bytes", value);

      assertThat((byte[]) cache.get("large-bytes"), is(value));
    } finally {
      CacheService.CACHE_SIZE_LIMIT = originalLimit;
    }
  }

  @Test
  public void putAndGet_largeObject_usesChunks() {
    int originalLimit = CacheService.CACHE_SIZE_LIMIT;
    CacheService.CACHE_SIZE_LIMIT = 4;
    try {
      CacheService cache = new CacheService();
      cache.useLocalCache(false);
      String value = "large serializable value";

      cache.put("large-object", value);

      assertThat((String) cache.get("large-object"), is(value));
    } finally {
      CacheService.CACHE_SIZE_LIMIT = originalLimit;
    }
  }

  @Test
  public void putAndGetAll_largeObject_usesChunks() {
    int originalLimit = CacheService.CACHE_SIZE_LIMIT;
    CacheService.CACHE_SIZE_LIMIT = 4;
    try {
      CacheService cache = new CacheService();
      cache.useLocalCache(false);
      String value = "large getAll value";

      cache.put("large-get-all-object", value);
      Map result = cache.getAll(Collections.singleton("large-get-all-object"));

      assertThat((String) result.get("large-get-all-object"), is(value));
    } finally {
      CacheService.CACHE_SIZE_LIMIT = originalLimit;
    }
  }

  @SuppressWarnings("unchecked")
  private Map<String, byte[]> localCache(CacheService cache) throws Exception {
    Field field = CacheService.class.getDeclaredField("localCache");
    field.setAccessible(true);
    return (Map<String, byte[]>) field.get(cache);
  }
}
