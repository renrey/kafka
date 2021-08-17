/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.utils.timer

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.concurrent.{DelayQueue, Executors, TimeUnit}

import kafka.utils.threadsafe
import org.apache.kafka.common.utils.{KafkaThread, Time}

trait Timer {
  /**
    * Add a new task to this executor. It will be executed after the task's delay
    * (beginning from the time of submission)
    * @param timerTask the task to add
    */
  def add(timerTask: TimerTask): Unit

  /**
    * Advance the internal clock, executing any tasks whose expiration has been
    * reached within the duration of the passed timeout.
    * @param timeoutMs
    * @return whether or not any tasks were executed
    */
  def advanceClock(timeoutMs: Long): Boolean

  /**
    * Get the number of tasks pending execution
    * @return the number of tasks
    */
  def size: Int

  /**
    * Shutdown the timer service, leaving pending tasks unexecuted
    */
  def shutdown(): Unit
}

@threadsafe
class SystemTimer(executorName: String,
                  tickMs: Long = 1,
                  wheelSize: Int = 20,
                  startMs: Long = Time.SYSTEM.hiResClockMs) extends Timer {

  /**
   * 到期后执行函数逻辑的线程池
   */
  // timeout timer
  private[this] val taskExecutor = Executors.newFixedThreadPool(1,
    (runnable: Runnable) => KafkaThread.nonDaemon("executor-" + executorName, runnable))

  /**
   * 用来堵塞下一次需要唤醒bucket的DelayQueue延时队列
   * 一个时间bucket只会放入一次
   */
  private[this] val delayQueue = new DelayQueue[TimerTaskList]()
  private[this] val taskCounter = new AtomicInteger(0)
  /**
   * 最低级的时间轮，间隔1ms，范围20ms
   */
  private[this] val timingWheel = new TimingWheel(
    tickMs = tickMs, // 默认间隔1ms
    wheelSize = wheelSize, // 默认entry数量20
    startMs = startMs, // 默认开始时间为创建时间
    taskCounter = taskCounter,
    delayQueue
  )

  // Locks used to protect data structures while ticking
  private[this] val readWriteLock = new ReentrantReadWriteLock()
  private[this] val readLock = readWriteLock.readLock()
  private[this] val writeLock = readWriteLock.writeLock()

  /**
   * 新增一个延时任务
   * @param timerTask the task to add
   */
  def add(timerTask: TimerTask): Unit = {
    readLock.lock()
    try {
      // 添加entry
      addTimerTaskEntry(new TimerTaskEntry(timerTask, timerTask.delayMs + Time.SYSTEM.hiResClockMs))
    } finally {
      readLock.unlock()
    }
  }

  private def addTimerTaskEntry(timerTaskEntry: TimerTaskEntry): Unit = {
    // 加入到timingWheel 时间轮
    if (!timingWheel.add(timerTaskEntry)) {
      // Already expired or cancelled
      /**
       * 到期，但不是被取消的任务，提交到线程执行
       * 实际上执行的入口只有这里！！！！！
       * 正常运转到到期，也是这里执行
       */
      if (!timerTaskEntry.cancelled)
        taskExecutor.submit(timerTaskEntry.timerTask)
    }
  }

  /*
   * Advances the clock if there is an expired bucket. If there isn't any expired bucket when called,
   * waits up to timeoutMs before giving up.
   */
  def advanceClock(timeoutMs: Long): Boolean = {
    /**
     * 从delayQueue堵塞等待取出bucket
     */
    var bucket = delayQueue.poll(timeoutMs, TimeUnit.MILLISECONDS)
    // 获取到过期的bucket
    if (bucket != null) {
      writeLock.lock()
      try {
        while (bucket != null) {
          /**
           * 时间轮更新currentTime当前时间
           * bucket.getExpiration: bucket过期时间，可以取出这个bucket，说明已经到达了这个时间了
           */
          timingWheel.advanceClock(bucket.getExpiration)
          /**
           * 遍历bucket的任务链表，每个任务重新插入到低级的时间轮或者执行
           */
          bucket.flush(addTimerTaskEntry)

          /**
           * 如果马上有下个bucket到期，直接执行下一次的advanceClock、flush，这样就不用重新进入advanceClock后再执行
           * 如果没有，就返回,进入下次advanceClock
           * 注意：如果一直有任务过期，无法往timer新的任务，因为这里是上了写锁，只能等到没有任务过期，解锁
           * 任务过期的优先级比新增任务高
           */
          bucket = delayQueue.poll()
        }
      } finally {
        writeLock.unlock()
      }
      true
    // 当前时间范围（timeoutMs）没有需要执行的，返回，进入下次advanceClock
    } else {
      false
    }
  }

  def size: Int = taskCounter.get

  override def shutdown(): Unit = {
    taskExecutor.shutdown()
  }

}
