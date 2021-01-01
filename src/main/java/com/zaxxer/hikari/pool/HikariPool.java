/*
 * Copyright (C) 2013,2014 Brett Wooldridge
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.zaxxer.hikari.pool;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariPoolMXBean;
import com.zaxxer.hikari.metrics.MetricsTrackerFactory;
import com.zaxxer.hikari.metrics.PoolStats;
import com.zaxxer.hikari.metrics.dropwizard.CodahaleHealthChecker;
import com.zaxxer.hikari.metrics.dropwizard.CodahaleMetricsTrackerFactory;
import com.zaxxer.hikari.metrics.micrometer.MicrometerMetricsTrackerFactory;
import com.zaxxer.hikari.util.ConcurrentBag;
import com.zaxxer.hikari.util.ConcurrentBag.IBagStateListener;
import com.zaxxer.hikari.util.SuspendResumeLock;
import com.zaxxer.hikari.util.UtilityElf.DefaultThreadFactory;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;

import static com.zaxxer.hikari.util.ClockSource.currentTime;
import static com.zaxxer.hikari.util.ClockSource.elapsedDisplayString;
import static com.zaxxer.hikari.util.ClockSource.elapsedMillis;
import static com.zaxxer.hikari.util.ClockSource.plusMillis;
import static com.zaxxer.hikari.util.ConcurrentBag.IConcurrentBagEntry.STATE_IN_USE;
import static com.zaxxer.hikari.util.ConcurrentBag.IConcurrentBagEntry.STATE_NOT_IN_USE;
import static com.zaxxer.hikari.util.UtilityElf.createThreadPoolExecutor;
import static com.zaxxer.hikari.util.UtilityElf.quietlySleep;
import static com.zaxxer.hikari.util.UtilityElf.safeIsAssignableFrom;
import static java.util.Collections.unmodifiableCollection;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * This is the primary connection pool class that provides the basic
 * pooling behavior for HikariCP.
 *
 * @author Brett Wooldridge
 */
public final class HikariPool extends PoolBase implements HikariPoolMXBean, IBagStateListener
{
   private final Logger logger = LoggerFactory.getLogger(HikariPool.class);

   public static final int POOL_NORMAL = 0;
   public static final int POOL_SUSPENDED = 1;
   public static final int POOL_SHUTDOWN = 2;

   public volatile int poolState;

   private final long aliveBypassWindowMs = Long.getLong("com.zaxxer.hikari.aliveBypassWindowMs", MILLISECONDS.toMillis(500));
   private final long housekeepingPeriodMs = Long.getLong("com.zaxxer.hikari.housekeeping.periodMs", SECONDS.toMillis(30));

   private static final String EVICTED_CONNECTION_MESSAGE = "(connection was evicted)";
   private static final String DEAD_CONNECTION_MESSAGE = "(connection is dead)";


   private final PoolEntryCreator poolEntryCreator = new PoolEntryCreator(null /*logging prefix*/);
   private final PoolEntryCreator postFillPoolEntryCreator = new PoolEntryCreator("After adding ");
   //connection Bag 取代BlockingQueue的并发安全容器，作者自己定制的容器
   private final ConcurrentBag<PoolEntry> connectionBag;



   private final Collection<Runnable> addConnectionQueueReadOnlyView;

   //addConnection Executor
   private final ThreadPoolExecutor addConnectionExecutor;
   //close Connection Executor
   private final ThreadPoolExecutor closeConnectionExecutor;

   //ProxyLeakTask的执行器工厂方法
   private final ProxyLeakTaskFactory leakTaskFactory;
   //作者写的一个锁 基于信号量
   private final SuspendResumeLock suspendResumeLock;


   //HouseKeeper任务执行器
   private final ScheduledExecutorService houseKeepingExecutorService;
   //HouseKeeper任务
   private ScheduledFuture<?> houseKeeperTask;

   /**
    * Construct a HikariPool with the specified configuration.
    *
    * @param config a HikariConfig instance
    */
   public HikariPool(final HikariConfig config)
   {
      super(config);

      this.connectionBag = new ConcurrentBag<>(this);
      this.suspendResumeLock = config.isAllowPoolSuspension() ? new SuspendResumeLock() : SuspendResumeLock.FAUX_LOCK;

      this.houseKeepingExecutorService = initializeHouseKeepingExecutorService();

      checkFailFast();

      if (config.getMetricsTrackerFactory() != null) {
         setMetricsTrackerFactory(config.getMetricsTrackerFactory());
      }
      else {
         setMetricRegistry(config.getMetricRegistry());
      }

      setHealthCheckRegistry(config.getHealthCheckRegistry());

      handleMBeans(this, true);

      ThreadFactory threadFactory = config.getThreadFactory();

      //池最大连接数设置
      final int maxPoolSize = config.getMaximumPoolSize();

      //初始化 addConnection执行器
      LinkedBlockingQueue<Runnable> addConnectionQueue = new LinkedBlockingQueue<>(maxPoolSize);
      this.addConnectionQueueReadOnlyView = unmodifiableCollection(addConnectionQueue);
      this.addConnectionExecutor = createThreadPoolExecutor(addConnectionQueue, poolName + " connection adder", threadFactory, new ThreadPoolExecutor.DiscardOldestPolicy());

      //初始化 closeConnection执行器
      this.closeConnectionExecutor = createThreadPoolExecutor(maxPoolSize, poolName + " connection closer", threadFactory, new ThreadPoolExecutor.CallerRunsPolicy());

      //初始化 ProxyLeakTask执行器
      this.leakTaskFactory = new ProxyLeakTaskFactory(config.getLeakDetectionThreshold(), houseKeepingExecutorService);

      //初始化 houseKeeperTask
      this.houseKeeperTask = houseKeepingExecutorService.scheduleWithFixedDelay(new HouseKeeper(), 100L, housekeepingPeriodMs, MILLISECONDS);

      if (Boolean.getBoolean("com.zaxxer.hikari.blockUntilFilled") && config.getInitializationFailTimeout() > 1) {
         //addConnectionExecutor 设置执行器的Worker线程数量
         //取16和可用处理器的最小数量
         addConnectionExecutor.setCorePoolSize(Math.min(16, Runtime.getRuntime().availableProcessors()));
         addConnectionExecutor.setMaximumPoolSize(Math.min(16, Runtime.getRuntime().availableProcessors()));

         final long startTime = currentTime();
         while (elapsedMillis(startTime) < config.getInitializationFailTimeout() && getTotalConnections() < config.getMinimumIdle()) {
            quietlySleep(MILLISECONDS.toMillis(100));
         }

         //addConnectionExecutor 设置核心工作线程数
         addConnectionExecutor.setCorePoolSize(1);
         addConnectionExecutor.setMaximumPoolSize(1);
      }
   }

   /**
    * Get a connection from the pool, or timeout after connectionTimeout milliseconds.
    *
    * @return a java.sql.Connection instance
    * @throws SQLException thrown if a timeout occurs trying to obtain a connection
    */
   public Connection getConnection() throws SQLException
   {
      return getConnection(connectionTimeout);
   }

   /**
    * Get a connection from the pool, or timeout after the specified number of milliseconds.
    *
    * @param hardTimeout the maximum time to wait for a connection from the pool
    * @return a java.sql.Connection instance
    * @throws SQLException thrown if a timeout occurs trying to obtain a connection
    */
   //TODO　获取连接并未做并发控制　　通过compareAndSet实现锁粒度的最小化？
   public Connection getConnection(final long hardTimeout) throws SQLException
   {

      //那就什么都不干了，直接放进来，不做suspend--做了一个类似限流的功能
      //后边的代码允许多线程访问。只不过加了流量的限制
      suspendResumeLock.acquire();
      final long startTime = currentTime();

      try {
         long timeout = hardTimeout;
         do {
            //先借一个PoolEntry
            PoolEntry poolEntry = connectionBag.borrow(timeout, MILLISECONDS);
            //如果没有借到 返回异常
            if (poolEntry == null) {
               break; // We timed out... break and throw exception
            }

            final long now = currentTime();
            //如果借到的Entry为空
            if (poolEntry.isMarkedEvicted() ||
               //或者 或者空闲时间过长&&已经not alive
               (elapsedMillis(poolEntry.lastAccessed, now) > aliveBypassWindowMs && !isConnectionAlive(poolEntry.connection))) {
               closeConnection(poolEntry, poolEntry.isMarkedEvicted() ? EVICTED_CONNECTION_MESSAGE : DEAD_CONNECTION_MESSAGE);
               timeout = hardTimeout - elapsedMillis(startTime);
            }
            else {
               metricsTracker.recordBorrowStats(poolEntry, startTime);
               return poolEntry.createProxyConnection(leakTaskFactory.schedule(poolEntry), now);
            }
         } while (timeout > 0L);

         metricsTracker.recordBorrowTimeoutStats(startTime);
         throw createTimeoutException(startTime);
      }
      catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw new SQLException(poolName + " - Interrupted during connection acquisition", e);
      }
      finally {
         //释放锁-》信号量释放许可
         suspendResumeLock.release();
      }
   }

   /**
    * Shutdown the pool, closing all idle connections and aborting or closing
    * active connections.
    *
    * @throws InterruptedException thrown if the thread is interrupted during shutdown
    */

   //synchronized 单线程访问
   public synchronized void shutdown() throws InterruptedException
   {
      try {
         //更改连接池状态
         poolState = POOL_SHUTDOWN;

         if (addConnectionExecutor == null) { // pool never started
            return;
         }

         logPoolState("Before shutdown ");

         if (houseKeeperTask != null) {
            houseKeeperTask.cancel(false);
            houseKeeperTask = null;
         }

         softEvictConnections();

         addConnectionExecutor.shutdown();
         addConnectionExecutor.awaitTermination(getLoginTimeout(), SECONDS);

         destroyHouseKeepingExecutorService();

         connectionBag.close();

         final ExecutorService assassinExecutor = createThreadPoolExecutor(config.getMaximumPoolSize(), poolName + " connection assassinator",
                                                                           config.getThreadFactory(), new ThreadPoolExecutor.CallerRunsPolicy());
         try {
            final long start = currentTime();
            do {
               abortActiveConnections(assassinExecutor);
               softEvictConnections();
            } while (getTotalConnections() > 0 && elapsedMillis(start) < SECONDS.toMillis(10));
         }
         finally {
            assassinExecutor.shutdown();
            assassinExecutor.awaitTermination(10L, SECONDS);
         }

         shutdownNetworkTimeoutExecutor();
         closeConnectionExecutor.shutdown();
         closeConnectionExecutor.awaitTermination(10L, SECONDS);
      }
      finally {
         logPoolState("After shutdown ");
         handleMBeans(this, false);
         metricsTracker.close();
      }
   }

   /**
    * Evict a Connection from the pool.
    *
    * @param connection the Connection to evict (actually a {@link ProxyConnection})
    */
   public void evictConnection(Connection connection)
   {
      ProxyConnection proxyConnection = (ProxyConnection) connection;
      proxyConnection.cancelLeakTask();

      try {
         softEvictConnection(proxyConnection.getPoolEntry(), "(connection evicted by user)", !connection.isClosed() /* owner */);
      }
      catch (SQLException e) {
         // unreachable in HikariCP, but we're still forced to catch it
      }
   }

   /**
    * Set a metrics registry to be used when registering metrics collectors.  The HikariDataSource prevents this
    * method from being called more than once.
    *
    * @param metricRegistry the metrics registry instance to use
    */
   public void setMetricRegistry(Object metricRegistry)
   {
      if (metricRegistry != null && safeIsAssignableFrom(metricRegistry, "com.codahale.metrics.MetricRegistry")) {
         setMetricsTrackerFactory(new CodahaleMetricsTrackerFactory((MetricRegistry) metricRegistry));
      }
      else if (metricRegistry != null && safeIsAssignableFrom(metricRegistry, "io.micrometer.core.instrument.MeterRegistry")) {
         setMetricsTrackerFactory(new MicrometerMetricsTrackerFactory((MeterRegistry) metricRegistry));
      }
      else {
         setMetricsTrackerFactory(null);
      }
   }

   /**
    * Set the MetricsTrackerFactory to be used to create the IMetricsTracker instance used by the pool.
    *
    * @param metricsTrackerFactory an instance of a class that subclasses MetricsTrackerFactory
    */
   public void setMetricsTrackerFactory(MetricsTrackerFactory metricsTrackerFactory)
   {
      if (metricsTrackerFactory != null) {
         this.metricsTracker = new MetricsTrackerDelegate(metricsTrackerFactory.create(config.getPoolName(), getPoolStats()));
      }
      else {
         this.metricsTracker = new NopMetricsTrackerDelegate();
      }
   }

   /**
    * Set the health check registry to be used when registering health checks.  Currently only Codahale health
    * checks are supported.
    *
    * @param healthCheckRegistry the health check registry instance to use
    */
   public void setHealthCheckRegistry(Object healthCheckRegistry)
   {
      if (healthCheckRegistry != null) {
         CodahaleHealthChecker.registerHealthChecks(this, config, (HealthCheckRegistry) healthCheckRegistry);
      }
   }

   // ***********************************************************************
   //                        IBagStateListener callback
   // ***********************************************************************

   /** {@inheritDoc} */
   @Override
   public void addBagItem(final int waiting)
   {
      final boolean shouldAdd = waiting - addConnectionQueueReadOnlyView.size() >= 0; // Yes, >= is intentional.
      if (shouldAdd) {
         addConnectionExecutor.submit(poolEntryCreator);
      }
      else {
         logger.debug("{} - Add connection elided, waiting {}, queue {}", poolName, waiting, addConnectionQueueReadOnlyView.size());
      }
   }

   // ***********************************************************************
   //                        HikariPoolMBean methods
   // ***********************************************************************

   /** {@inheritDoc} */
   @Override
   public int getActiveConnections()
   {
      return connectionBag.getCount(STATE_IN_USE);
   }

   /** {@inheritDoc} */
   @Override
   public int getIdleConnections()
   {
      return connectionBag.getCount(STATE_NOT_IN_USE);
   }

   /** {@inheritDoc} */
   @Override
   public int getTotalConnections()
   {
      return connectionBag.size();
   }

   /** {@inheritDoc} */
   @Override
   public int getThreadsAwaitingConnection()
   {
      return connectionBag.getWaitingThreadCount();
   }

   /** {@inheritDoc} */
   @Override
   public void softEvictConnections()
   {
      connectionBag.values().forEach(poolEntry -> softEvictConnection(poolEntry, "(connection evicted)", false /* not owner */));
   }

   /** {@inheritDoc} */
   @Override
   public synchronized void suspendPool()
   {
      if (suspendResumeLock == SuspendResumeLock.FAUX_LOCK) {
         throw new IllegalStateException(poolName + " - is not suspendable");
      }
      else if (poolState != POOL_SUSPENDED) {
         suspendResumeLock.suspend();
         poolState = POOL_SUSPENDED;
      }
   }

   /** {@inheritDoc} */
   @Override
   public synchronized void resumePool()
   {
      if (poolState == POOL_SUSPENDED) {
         poolState = POOL_NORMAL;
         fillPool();
         suspendResumeLock.resume();
      }
   }

   // ***********************************************************************
   //                           Package methods
   // ***********************************************************************

   /**
    * Log the current pool state at debug level.
    *
    * @param prefix an optional prefix to prepend the log message
    */
   void logPoolState(String... prefix)
   {
      if (logger.isDebugEnabled()) {
         logger.debug("{} - {}stats (total={}, active={}, idle={}, waiting={})",
                      poolName, (prefix.length > 0 ? prefix[0] : ""),
                      getTotalConnections(), getActiveConnections(), getIdleConnections(), getThreadsAwaitingConnection());
      }
   }

   /**
    * Recycle PoolEntry (add back to the pool)
    *
    * @param poolEntry the PoolEntry to recycle
    */
   @Override
   void recycle(final PoolEntry poolEntry)
   {
      metricsTracker.recordConnectionUsage(poolEntry);

      connectionBag.requite(poolEntry);
   }

   /**
    * Permanently close the real (underlying) connection (eat any exception).
    *
    * @param poolEntry poolEntry having the connection to close
    * @param closureReason reason to close
    */
   void closeConnection(final PoolEntry poolEntry, final String closureReason)
   {
      //容器移除Entry
      if (connectionBag.remove(poolEntry)) {
         //entry关闭
         final Connection connection = poolEntry.close();
         //closeConnectionExecutor 执行任务（采用异步执行的模式）
         closeConnectionExecutor.execute(() -> {
            //轻轻地关闭连接，而不是直接关闭连接
            quietlyCloseConnection(connection, closureReason);
            if (poolState == POOL_NORMAL) {
               fillPool();
            }
         });
      }
   }

   @SuppressWarnings("unused")
   int[] getPoolStateCounts()
   {
      return connectionBag.getStateCounts();
   }


   // ***********************************************************************
   //                           Private methods
   // ***********************************************************************

   /**
    * Creating new poolEntry.  If maxLifetime is configured, create a future End-of-life task with 2.5% variance from
    * the maxLifetime time to ensure there is no massive die-off of Connections in the pool.
    */
   private PoolEntry createPoolEntry()
   {
      try {
         final PoolEntry poolEntry = newPoolEntry();

         final long maxLifetime = config.getMaxLifetime();
         if (maxLifetime > 0) {
            // variance up to 2.5% of the maxlifetime
            final long variance = maxLifetime > 10_000 ? ThreadLocalRandom.current().nextLong( maxLifetime / 40 ) : 0;
            final long lifetime = maxLifetime - variance;
            poolEntry.setFutureEol(houseKeepingExecutorService.schedule(
               () -> {
                  if (softEvictConnection(poolEntry, "(connection has passed maxLifetime)", false /* not owner */)) {
                     addBagItem(connectionBag.getWaitingThreadCount());
                  }
               },
               lifetime, MILLISECONDS));
         }

         return poolEntry;
      }
      catch (ConnectionSetupException e) {
         if (poolState == POOL_NORMAL) { // we check POOL_NORMAL to avoid a flood of messages if shutdown() is running concurrently
            logger.error("{} - Error thrown while acquiring connection from data source", poolName, e.getCause());
            lastConnectionFailure.set(e);
         }
      }
      catch (Exception e) {
         if (poolState == POOL_NORMAL) { // we check POOL_NORMAL to avoid a flood of messages if shutdown() is running concurrently
            logger.debug("{} - Cannot acquire connection from data source", poolName, e);
         }
      }

      return null;
   }

   /**
    * Fill pool up from current idle connections (as they are perceived at the point of execution) to minimumIdle connections.
    */
   private synchronized void fillPool()
   {
      final int connectionsToAdd = Math.min(config.getMaximumPoolSize() - getTotalConnections(), config.getMinimumIdle() - getIdleConnections())
                                   - addConnectionQueueReadOnlyView.size();
      if (connectionsToAdd <= 0) logger.debug("{} - Fill pool skipped, pool is at sufficient level.", poolName);

      for (int i = 0; i < connectionsToAdd; i++) {
         //异步执行addConnection-(异步执行模式)
         addConnectionExecutor.submit((i < connectionsToAdd - 1) ? poolEntryCreator : postFillPoolEntryCreator);
      }
   }

   /**
    * Attempt to abort or close active connections.
    *
    * @param assassinExecutor the ExecutorService to pass to Connection.abort()
    */
   private void abortActiveConnections(final ExecutorService assassinExecutor)
   {
      for (PoolEntry poolEntry : connectionBag.values(STATE_IN_USE)) {
         Connection connection = poolEntry.close();
         try {
            connection.abort(assassinExecutor);
         }
         catch (Throwable e) {
            quietlyCloseConnection(connection, "(connection aborted during shutdown)");
         }
         finally {
            connectionBag.remove(poolEntry);
         }
      }
   }

   /**
    * If initializationFailFast is configured, check that we have DB connectivity.
    *
    * @throws PoolInitializationException if fails to create or validate connection
    * @see HikariConfig#setInitializationFailTimeout(long)
    */
   private void checkFailFast()
   {
      final long initializationTimeout = config.getInitializationFailTimeout();
      if (initializationTimeout < 0) {
         return;
      }

      final long startTime = currentTime();
      do {
         final PoolEntry poolEntry = createPoolEntry();
         if (poolEntry != null) {
            if (config.getMinimumIdle() > 0) {
               connectionBag.add(poolEntry);
               logger.debug("{} - Added connection {}", poolName, poolEntry.connection);
            }
            else {
               quietlyCloseConnection(poolEntry.close(), "(initialization check complete and minimumIdle is zero)");
            }

            return;
         }

         if (getLastConnectionFailure() instanceof ConnectionSetupException) {
            throwPoolInitializationException(getLastConnectionFailure().getCause());
         }

         quietlySleep(SECONDS.toMillis(1));
      } while (elapsedMillis(startTime) < initializationTimeout);

      if (initializationTimeout > 0) {
         throwPoolInitializationException(getLastConnectionFailure());
      }
   }

   /**
    * Log the Throwable that caused pool initialization to fail, and then throw a PoolInitializationException with
    * that cause attached.
    *
    * @param t the Throwable that caused the pool to fail to initialize (possibly null)
    */
   private void throwPoolInitializationException(Throwable t)
   {
      logger.error("{} - Exception during pool initialization.", poolName, t);
      destroyHouseKeepingExecutorService();
      throw new PoolInitializationException(t);
   }

   /**
    * "Soft" evict a Connection (/PoolEntry) from the pool.  If this method is being called by the user directly
    * through {@link com.zaxxer.hikari.HikariDataSource#evictConnection(Connection)} then {@code owner} is {@code true}.
    *
    * If the caller is the owner, or if the Connection is idle (i.e. can be "reserved" in the {@link ConcurrentBag}),
    * then we can close the connection immediately.  Otherwise, we leave it "marked" for eviction so that it is evicted
    * the next time someone tries to acquire it from the pool.
    *
    * @param poolEntry the PoolEntry (/Connection) to "soft" evict from the pool
    * @param reason the reason that the connection is being evicted
    * @param owner true if the caller is the owner of the connection, false otherwise
    * @return true if the connection was evicted (closed), false if it was merely marked for eviction
    */
   private boolean softEvictConnection(final PoolEntry poolEntry, final String reason, final boolean owner)
   {
      //entry标记被清理
      poolEntry.markEvicted();
      //reserve成功
      if (owner || connectionBag.reserve(poolEntry)) {
         //关闭连接
         closeConnection(poolEntry, reason);
         return true;
      }

      return false;
   }

   /**
    * Create/initialize the Housekeeping service {@link ScheduledExecutorService}.  If the user specified an Executor
    * to be used in the {@link HikariConfig}, then we use that.  If no Executor was specified (typical), then create
    * an Executor and configure it.
    *
    * @return either the user specified {@link ScheduledExecutorService}, or the one we created
    */
   private ScheduledExecutorService initializeHouseKeepingExecutorService()
   {
      if (config.getScheduledExecutor() == null) {
         final ThreadFactory threadFactory = Optional.ofNullable(config.getThreadFactory()).orElseGet(() -> new DefaultThreadFactory(poolName + " housekeeper", true));
         final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, threadFactory, new ThreadPoolExecutor.DiscardPolicy());
         executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
         executor.setRemoveOnCancelPolicy(true);
         return executor;
      }
      else {
         return config.getScheduledExecutor();
      }
   }

   /**
    * Destroy (/shutdown) the Housekeeping service Executor, if it was the one that we created.
    */
   private void destroyHouseKeepingExecutorService()
   {
      if (config.getScheduledExecutor() == null) {
         houseKeepingExecutorService.shutdownNow();
      }
   }

   /**
    * Create a PoolStats instance that will be used by metrics tracking, with a pollable resolution of 1 second.
    *
    * @return a PoolStats instance
    */
   private PoolStats getPoolStats()
   {
      return new PoolStats(SECONDS.toMillis(1)) {
         @Override
         protected void update() {
            this.pendingThreads = HikariPool.this.getThreadsAwaitingConnection();
            this.idleConnections = HikariPool.this.getIdleConnections();
            this.totalConnections = HikariPool.this.getTotalConnections();
            this.activeConnections = HikariPool.this.getActiveConnections();
            this.maxConnections = config.getMaximumPoolSize();
            this.minConnections = config.getMinimumIdle();
         }
      };
   }

   /**
    * Create a timeout exception (specifically, {@link SQLTransientConnectionException}) to be thrown, because a
    * timeout occurred when trying to acquire a Connection from the pool.  If there was an underlying cause for the
    * timeout, e.g. a SQLException thrown by the driver while trying to create a new Connection, then use the
    * SQL State from that exception as our own and additionally set that exception as the "next" SQLException inside
    * of our exception.
    *
    * As a side-effect, log the timeout failure at DEBUG, and record the timeout failure in the metrics tracker.
    *
    * @param startTime the start time (timestamp) of the acquisition attempt
    * @return a SQLException to be thrown from {@link #getConnection()}
    */
   private SQLException createTimeoutException(long startTime)
   {
      logPoolState("Timeout failure ");
      metricsTracker.recordConnectionTimeout();

      String sqlState = null;
      final Throwable originalException = getLastConnectionFailure();
      if (originalException instanceof SQLException) {
         sqlState = ((SQLException) originalException).getSQLState();
      }
      final SQLException connectionException = new SQLTransientConnectionException(poolName + " - Connection is not available, request timed out after " + elapsedMillis(startTime) + "ms.", sqlState, originalException);
      if (originalException instanceof SQLException) {
         connectionException.setNextException((SQLException) originalException);
      }

      return connectionException;
   }


   // ***********************************************************************
   //                      Non-anonymous Inner-classes
   // ***********************************************************************

   /**
    * Creating and adding poolEntries (connections) to the pool.
    */
   private final class PoolEntryCreator implements Callable<Boolean>
   {
      private final String loggingPrefix;

      PoolEntryCreator(String loggingPrefix)
      {
         this.loggingPrefix = loggingPrefix;
      }

      @Override
      public Boolean call()
      {
         long sleepBackoff = 250L;
         while (poolState == POOL_NORMAL && shouldCreateAnotherConnection()) {
            final PoolEntry poolEntry = createPoolEntry();
            if (poolEntry != null) {
               connectionBag.add(poolEntry);
               logger.debug("{} - Added connection {}", poolName, poolEntry.connection);
               if (loggingPrefix != null) {
                  logPoolState(loggingPrefix);
               }
               return Boolean.TRUE;
            }

            // failed to get connection from db, sleep and retry
            if (loggingPrefix != null) logger.debug("{} - Connection add failed, sleeping with backoff: {}ms", poolName, sleepBackoff);
            quietlySleep(sleepBackoff);
            sleepBackoff = Math.min(SECONDS.toMillis(10), Math.min(connectionTimeout, (long) (sleepBackoff * 1.5)));
         }

         // Pool is suspended or shutdown or at max size
         return Boolean.FALSE;
      }

      /**
       * We only create connections if we need another idle connection or have threads still waiting
       * for a new connection.  Otherwise we bail out of the request to create.
       *
       * @return true if we should create a connection, false if the need has disappeared
       */
      private synchronized boolean shouldCreateAnotherConnection() {
         return getTotalConnections() < config.getMaximumPoolSize() &&
            (connectionBag.getWaitingThreadCount() > 0 || getIdleConnections() < config.getMinimumIdle());
      }
   }

   /**
    * The house keeping task to retire and maintain minimum idle connections.
    */
   //淘汰链接、维持最小Idle连接 任务
   private final class HouseKeeper implements Runnable
   {
      private volatile long previous = plusMillis(currentTime(), -housekeepingPeriodMs);

      @Override
      public void run()
      {
         try {
            // refresh values in case they changed via MBean
            connectionTimeout = config.getConnectionTimeout();
            validationTimeout = config.getValidationTimeout();
            leakTaskFactory.updateLeakDetectionThreshold(config.getLeakDetectionThreshold());
            catalog = (config.getCatalog() != null && !config.getCatalog().equals(catalog)) ? config.getCatalog() : catalog;

            final long idleTimeout = config.getIdleTimeout();
            final long now = currentTime();
            //如果 检测到时钟回拨
            // Detect retrograde time, allowing +128ms as per NTP spec.
            //当前时间+128毫秒 小于 previous+housekeepingPeriodMs 则认为发生了时钟回拨 “在获取now的之前的这段时间，表走慢了”
            if (plusMillis(now, 128) < plusMillis(previous, housekeepingPeriodMs)) {
               logger.warn("{} - Retrograde clock change detected (housekeeper delta={}), soft-evicting connections from pool.",
                           poolName, elapsedDisplayString(previous, now));
               previous = now;
               //如果时钟发生回拨，则全量进行挨个扫描，进行柔和地清理联接。（这里算是对始终回拨情况的一种应急方式）
               softEvictConnections();
               return;
            }
            //否则 如果现在时间now大于 previous+(3 * housekeepingPeriodMs) / 2) 则认为现在时间超了，“在获取now的之前的这段时间，表走快了”
            else if (now > plusMillis(previous, (3 * housekeepingPeriodMs) / 2)) {
               // No point evicting for forward clock motion, this merely accelerates connection retirement anyway
               logger.warn("{} - Thread starvation or clock leap detected (housekeeper delta={}).", poolName, elapsedDisplayString(previous, now));
            }

            previous = now;

            String afterPrefix = "Pool ";
            if (idleTimeout > 0L && config.getMinimumIdle() < config.getMaximumPoolSize()) {
               logPoolState("Before cleanup ");
               afterPrefix = "After cleanup  ";

               //获取没有在使用状态中的所有元素
               final List<PoolEntry> notInUse = connectionBag.values(STATE_NOT_IN_USE);
               //排除MinimumIdle之后，剩余可移除的数量
               int toRemove = notInUse.size() - config.getMinimumIdle();
               for (PoolEntry entry : notInUse) {
                  if (toRemove > 0 &&
                     //元素空闲时间超过规定，并且当前线程争取到entry的所有权（通过标记其状态来获取）
                     elapsedMillis(entry.lastAccessed, now) > idleTimeout && connectionBag.reserve(entry)) {
                     //然后，关闭连接
                     closeConnection(entry, "(connection has passed idleTimeout)");
                     toRemove--;
                  }
               }
            }

            logPoolState(afterPrefix);

            fillPool(); // Try to maintain minimum connections
         }
         catch (Exception e) {
            logger.error("Unexpected exception in housekeeping task", e);
         }
      }
   }

   public static class PoolInitializationException extends RuntimeException
   {
      private static final long serialVersionUID = 929872118275916520L;

      /**
       * Construct an exception, possibly wrapping the provided Throwable as the cause.
       * @param t the Throwable to wrap
       */
      public PoolInitializationException(Throwable t)
      {
         super("Failed to initialize pool: " + t.getMessage(), t);
      }
   }
}
