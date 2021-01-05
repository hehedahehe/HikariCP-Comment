/*
 * Copyright (C) 2013 Brett Wooldridge
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

import static com.zaxxer.hikari.pool.TestElf.newHikariConfig;
import static com.zaxxer.hikari.pool.TestElf.getPool;
import static com.zaxxer.hikari.pool.TestElf.setConfigUnitTest;
import static com.zaxxer.hikari.pool.TestElf.setSlf4jLogLevel;
import static com.zaxxer.hikari.pool.TestElf.setSlf4jTargetStream;
import static com.zaxxer.hikari.util.UtilityElf.createInstance;
import static com.zaxxer.hikari.util.UtilityElf.getTransactionIsolation;
import static com.zaxxer.hikari.util.UtilityElf.quietlySleep;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Level;
import org.junit.Test;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

/**java.lang.IllegalStateException:
 * You need to run the CLI build and you need target/classes in your classpath to run.

 at com.zaxxer.hikari.pool.ProxyFactory.getProxyConnection(ProxyFactory.java:58)
 at com.zaxxer.hikari.pool.PoolEntry.createProxyConnection(PoolEntry.java:97)
 at com.zaxxer.hikari.pool.HikariPool.getConnection(HikariPool.java:227)
 at com.zaxxer.hikari.pool.HikariPool.getConnection(HikariPool.java:182)
 at com.zaxxer.hikari.HikariDataSource.getConnection(HikariDataSource.java:100)
 at com.zaxxer.hikari.pool.MiscTest.testLeakDetection(MiscTest.java:118)
 at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
 at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
 at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
 at java.lang.reflect.Method.invoke(Method.java:498)
 at org.junit.runners.model.FrameworkMethod$1.runReflectiveCall(FrameworkMethod.java:50)
 at org.junit.internal.runners.model.ReflectiveCallable.run(ReflectiveCallable.java:12)
 at org.junit.runners.model.FrameworkMethod.invokeExplosively(FrameworkMethod.java:47)
 at org.junit.internal.runners.statements.InvokeMethod.evaluate(InvokeMethod.java:17)
 at org.junit.runners.ParentRunner.runLeaf(ParentRunner.java:325)
 at org.junit.runners.BlockJUnit4ClassRunner.runChild(BlockJUnit4ClassRunner.java:78)
 at org.junit.runners.BlockJUnit4ClassRunner.runChild(BlockJUnit4ClassRunner.java:57)
 at org.junit.runners.ParentRunner$3.run(ParentRunner.java:290)
 at org.junit.runners.ParentRunner$1.schedule(ParentRunner.java:71)
 at org.junit.runners.ParentRunner.runChildren(ParentRunner.java:288)
 at org.junit.runners.ParentRunner.access$000(ParentRunner.java:58)
 at org.junit.runners.ParentRunner$2.evaluate(ParentRunner.java:268)
 at org.junit.runners.ParentRunner.run(ParentRunner.java:363)
 at org.junit.runner.JUnitCore.run(JUnitCore.java:137)
 at com.intellij.junit4.JUnit4IdeaTestRunner.startRunnerWithArgs(JUnit4IdeaTestRunner.java:69)
 at com.intellij.rt.junit.IdeaTestRunner$Repeater.startRunnerWithArgs(IdeaTestRunner.java:33)
 at com.intellij.rt.junit.JUnitStarter.prepareStreamsAndStart(JUnitStarter.java:220)
 at com.intellij.rt.junit.JUnitStarter.main(JUnitStarter.java:53)

 * @author Brett Wooldridge
 */
public class MiscTest
{
   @Test
   public void testLogWriter() throws SQLException
   {
      HikariConfig config = newHikariConfig();
      config.setMinimumIdle(0);
      config.setMaximumPoolSize(4);
      config.setDataSourceClassName("com.zaxxer.hikari.mocks.StubDataSource");
      setConfigUnitTest(true);

      try (HikariDataSource ds = new HikariDataSource(config)) {
         PrintWriter writer = new PrintWriter(System.out);
         ds.setLogWriter(writer);
         assertSame(writer, ds.getLogWriter());
         assertEquals("testLogWriter", config.getPoolName());
      }
      finally
      {
         setConfigUnitTest(false);
      }
   }

   @Test
   public void testInvalidIsolation()
   {
      try {
         getTransactionIsolation("INVALID");
         fail();
      }
      catch (Exception e) {
         assertTrue(e instanceof IllegalArgumentException);
      }
   }

   @Test
   public void testCreateInstance()
   {
      try {
         createInstance("invalid", null);
         fail();
      }
      catch (RuntimeException e) {
         assertTrue(e.getCause() instanceof ClassNotFoundException);
      }
   }

   @Test
   public void testLeakDetection() throws Exception
   {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try (PrintStream ps = new PrintStream(baos, true)) {
         setSlf4jTargetStream(Class.forName("com.zaxxer.hikari.pool.ProxyLeakTask"), ps);
         setConfigUnitTest(true);

         HikariConfig config = newHikariConfig();
         config.setMinimumIdle(0);
         config.setMaximumPoolSize(4);
         config.setThreadFactory(Executors.defaultThreadFactory());
         config.setMetricRegistry(null);
         config.setLeakDetectionThreshold(TimeUnit.SECONDS.toMillis(1));
         config.setDataSourceClassName("com.zaxxer.hikari.mocks.StubDataSource");

         try (HikariDataSource ds = new HikariDataSource(config)) {
            setSlf4jLogLevel(HikariPool.class, Level.DEBUG);
            getPool(ds).logPoolState();

            try (Connection connection = ds.getConnection()) {
               quietlySleep(SECONDS.toMillis(4));
               connection.close();
               quietlySleep(SECONDS.toMillis(1));
               ps.close();
               String s = new String(baos.toByteArray());
               assertNotNull("Exception string was null", s);
               assertTrue("Expected exception to contain 'Connection leak detection' but contains *" + s + "*", s.contains("Connection leak detection"));
            }
         }
         finally
         {
            setConfigUnitTest(false);
            setSlf4jLogLevel(HikariPool.class, Level.INFO);
         }
      }
   }
}
