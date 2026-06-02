package jp.honestyworks.pbcache;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.util.Collections;
import java.util.Map;
import org.junit.Test;
import org.slim3.tester.AppEngineTestCase;

/**
 * {@link CacheService} のテストケース。
 */
public class CacheServiceTest extends AppEngineTestCase {

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
}
