/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.io.hfile.bucket;

import static org.apache.hadoop.hbase.io.hfile.CacheConfig.BUCKETCACHE_PERSIST_INTERVAL_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;
import org.apache.hadoop.hbase.io.hfile.CacheTestUtils;
import org.apache.hadoop.hbase.io.hfile.Cacheable;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for HBASE-29857: BucketCache recovery with empty persistence file.
 * <p>
 * When BucketCache is empty (backingMap.size() == 0) at shutdown, the persistence file is written
 * with numChunks = 0. On recovery, the code must handle this case correctly without NPE.
 */
@Category({ IOTests.class, SmallTests.class })
public class TestBucketCacheEmptyPersistence {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestBucketCacheEmptyPersistence.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestBucketCacheEmptyPersistence.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static final long CAPACITY_SIZE = 32 * 1024 * 1024;
  private static final int BLOCK_SIZE = 8192;
  private static final int[] BUCKET_SIZES = new int[] { 8 * 1024 + 1024 };
  private static final int WRITER_THREADS = BucketCache.DEFAULT_WRITER_THREADS;
  private static final int WRITER_QUEUE_LEN = BucketCache.DEFAULT_WRITER_QUEUE_ITEMS;

  private Path testDir;
  private Configuration conf;

  @Before
  public void setUp() throws Exception {
    testDir = TEST_UTIL.getDataTestDir();
    TEST_UTIL.getTestFileSystem().mkdirs(testDir);
    conf = HBaseConfiguration.create();
    // Disable the persister thread to have full control over persistence timing
    conf.setLong(BUCKETCACHE_PERSIST_INTERVAL_KEY, Long.MAX_VALUE);
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.cleanupTestDir();
  }

  /**
   * Test that BucketCache can recover from a persistence file written when the cache was empty.
   * <p>
   * This reproduces HBASE-29857 where an empty cache would write numChunks=0 to the persistence
   * file, but on recovery the code would still try to read a first chunk that doesn't exist,
   * causing an NPE.
   */
  @Test
  public void testRecoveryFromEmptyCache() throws Exception {
    String persistencePath = testDir + "/bucket.persistence";
    String cachePath = "file:" + testDir + "/bucket.cache";

    // Create a BucketCache but don't add any blocks - it remains empty
    BucketCache bucketCache = new BucketCache(cachePath, CAPACITY_SIZE, BLOCK_SIZE, BUCKET_SIZES,
      WRITER_THREADS, WRITER_QUEUE_LEN, persistencePath,
      BucketCache.DEFAULT_ERROR_TOLERATION_DURATION, conf);

    // Verify cache is empty
    assertEquals("Cache should be empty", 0, bucketCache.backingMap.size());

    // Persist the empty cache state
    bucketCache.persistToFile();

    // Verify persistence file was created
    File persistenceFile = new File(persistencePath);
    assertTrue("Persistence file should exist", persistenceFile.exists());
    LOG.info("Empty cache persisted, file size: {} bytes", persistenceFile.length());

    // Shutdown the original cache
    bucketCache.shutdown();

    // Create a new BucketCache that recovers from the persistence file.
    // Before the fix for HBASE-29857, this would throw NPE in retrieveChunkedBackingMap
    // because it tried to read a first chunk that doesn't exist when numChunks=0.
    BucketCache recoveredCache = new BucketCache(cachePath, CAPACITY_SIZE, BLOCK_SIZE, BUCKET_SIZES,
      WRITER_THREADS, WRITER_QUEUE_LEN, persistencePath,
      BucketCache.DEFAULT_ERROR_TOLERATION_DURATION, conf);

    // Wait for backing map validation to complete
    while (!recoveredCache.getBackingMapValidated().get()) {
      Thread.sleep(10);
    }

    // Verify recovered cache is empty and functional
    assertEquals("Recovered cache should be empty", 0, recoveredCache.backingMap.size());

    // Verify we can still add blocks to the recovered cache
    CacheTestUtils.HFileBlockPair[] blocks = CacheTestUtils.generateHFileBlocks(BLOCK_SIZE, 1);
    cacheAndWaitUntilFlushedToBucket(recoveredCache, blocks[0].getBlockName(),
      blocks[0].getBlock());
    assertEquals("Should have 1 block after caching", 1, recoveredCache.backingMap.size());

    recoveredCache.shutdown();
  }

  /**
   * Test recovery from a cache that had blocks, was persisted, then all blocks were evicted before
   * another persistence. This ensures the empty state is handled correctly even when the cache was
   * previously non-empty.
   */
  @Test
  public void testRecoveryAfterEvictingAllBlocks() throws Exception {
    String persistencePath = testDir + "/bucket.persistence";
    String cachePath = "file:" + testDir + "/bucket.cache";

    BucketCache bucketCache = new BucketCache(cachePath, CAPACITY_SIZE, BLOCK_SIZE, BUCKET_SIZES,
      WRITER_THREADS, WRITER_QUEUE_LEN, persistencePath,
      BucketCache.DEFAULT_ERROR_TOLERATION_DURATION, conf);

    // Add some blocks
    CacheTestUtils.HFileBlockPair[] blocks = CacheTestUtils.generateHFileBlocks(BLOCK_SIZE, 3);
    for (CacheTestUtils.HFileBlockPair block : blocks) {
      cacheAndWaitUntilFlushedToBucket(bucketCache, block.getBlockName(), block.getBlock());
    }
    assertEquals("Should have 3 blocks", 3, bucketCache.backingMap.size());

    // Evict all blocks
    for (CacheTestUtils.HFileBlockPair block : blocks) {
      bucketCache.evictBlock(block.getBlockName());
    }
    assertEquals("Cache should be empty after eviction", 0, bucketCache.backingMap.size());

    // Persist the now-empty cache
    bucketCache.persistToFile();
    bucketCache.shutdown();

    // Recovery should succeed
    BucketCache recoveredCache = new BucketCache(cachePath, CAPACITY_SIZE, BLOCK_SIZE, BUCKET_SIZES,
      WRITER_THREADS, WRITER_QUEUE_LEN, persistencePath,
      BucketCache.DEFAULT_ERROR_TOLERATION_DURATION, conf);

    while (!recoveredCache.getBackingMapValidated().get()) {
      Thread.sleep(10);
    }

    assertEquals("Recovered cache should be empty", 0, recoveredCache.backingMap.size());
    recoveredCache.shutdown();
  }

  private void cacheAndWaitUntilFlushedToBucket(BucketCache cache, BlockCacheKey cacheKey,
    Cacheable block) throws InterruptedException {
    cache.cacheBlock(cacheKey, block);
    while (!cache.backingMap.containsKey(cacheKey) || cache.ramCache.containsKey(cacheKey)) {
      Thread.sleep(100);
    }
  }
}
