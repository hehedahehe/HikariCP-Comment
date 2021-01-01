/*
 * Copyright (C) 2013, 2014 Brett Wooldridge
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

package com.zaxxer.hikari.util;

import java.sql.SQLException;
import java.sql.SQLTransientException;
import java.util.concurrent.Semaphore;

/**
 * This class implements a lock that can be used to suspend and resume the pool.  It
 * also provides a faux implementation that is used when the feature is disabled that
 * hopefully gets fully "optimized away" by the JIT.
 *
 * @author Brett Wooldridge
 */
public class SuspendResumeLock
{
   //TODO　FAUX 模式???
   public static final SuspendResumeLock FAUX_LOCK = new SuspendResumeLock(false) {
      @Override
      public void acquire() {}

      @Override
      public void release() {}

      @Override
      public void suspend() {}

      @Override
      public void resume() {}
   };

   //声明信号量以及信号量对应的许可
   private static final int MAX_PERMITS = 10000;
   private final Semaphore acquisitionSemaphore;

   /**
    * Default constructor
    */
   public SuspendResumeLock()
   {
      this(true);
   }

   private SuspendResumeLock(final boolean createSemaphore)
   {
      acquisitionSemaphore = (createSemaphore ? new Semaphore(MAX_PERMITS, true) : null);
   }

   public void acquire() throws SQLException{
      //1. 如果获取成功 则直接返回
      if (acquisitionSemaphore.tryAcquire()) {
         return;
      }
      //2. 若被暂停了 则抛出暂停异常
      else if (Boolean.getBoolean("com.zaxxer.hikari.throwIfSuspended")) {
         throw new SQLTransientException("The pool is currently suspended and configured to throw exceptions upon acquisition");
      }
      //3. 继续进行获取
      acquisitionSemaphore.acquireUninterruptibly();
   }

   public void release()
   {
      acquisitionSemaphore.release();
   }

   public void suspend()
   {
      acquisitionSemaphore.acquireUninterruptibly(MAX_PERMITS);
   }

   public void resume()
   {
      acquisitionSemaphore.release(MAX_PERMITS);
   }
}
