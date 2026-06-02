package org.slim3.tester;

import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalMemcacheServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import org.junit.After;
import org.junit.Before;
import org.slim3.datastore.DatastoreUtil;

public abstract class AppEngineTestCase {

  private final LocalServiceTestHelper helper =
      new LocalServiceTestHelper(
          new LocalDatastoreServiceTestConfig()
              .setStoreDelayMs(0)
              .setDefaultHighRepJobPolicyUnappliedJobPercentage(0),
          new LocalMemcacheServiceTestConfig());

  @Before
  public void setUp() throws Exception {
    helper.setUp();
  }

  @After
  public void tearDown() throws Exception {
    helper.tearDown();
    DatastoreUtil.clearKeysCache();
  }
}
