package net.vvakame.memvache;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import com.google.appengine.api.datastore.EntityTranslatorPublic;
import com.google.appengine.api.datastore.Key;
import com.google.apphosting.api.DatastorePb.GetResponse;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import org.slim3.datastore.Datastore;
import org.slim3.tester.AppEngineTestCase;

public class MemcacheKeyUtilTest extends AppEngineTestCase {

  @Test
  public void conv_nullMap() {
    assertThat(MemcacheKeyUtil.conv(null).isEmpty(), is(true));
  }

  @Test
  public void conv_emptyPropertyEntityIsValid() {
    Key key = Datastore.createKey("hoge", 1);
    com.google.appengine.api.datastore.Entity entity =
        new com.google.appengine.api.datastore.Entity(key);
    GetResponse.Entity cached = new GetResponse.Entity();
    cached.setEntity(EntityTranslatorPublic.convertToPb(entity));

    Map<Key, Object> map = new HashMap<Key, Object>();
    map.put(key, cached);

    Map<Key, GetResponse.Entity> converted = MemcacheKeyUtil.conv(map);

    assertThat(converted.size(), is(1));
    assertThat(converted.get(key), sameInstance(cached));
  }
}
