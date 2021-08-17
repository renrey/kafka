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

import kafka.utils.nonthreadsafe

import java.util.concurrent.DelayQueue
import java.util.concurrent.atomic.AtomicInteger

/*
 * Hierarchical Timing Wheels
 *
 * A simple timing wheel is a circular list of buckets of timer tasks. Let u be the time unit.
 * A timing wheel with size n has n buckets and can hold timer tasks in n * u time interval.
 * Each bucket holds timer tasks that fall into the corresponding time range. At the beginning,
 * the first bucket holds tasks for [0, u), the second bucket holds tasks for [u, 2u), …,
 * the n-th bucket for [u * (n -1), u * n). Every interval of time unit u, the timer ticks and
 * moved to the next bucket then expire all timer tasks in it. So, the timer never insert a task
 * into the bucket for the current time since it is already expired. The timer immediately runs
 * the expired task. The emptied bucket is then available for the next round, so if the current
 * bucket is for the time t, it becomes the bucket for [t + u * n, t + (n + 1) * u) after a tick.
 * A timing wheel has O(1) cost for insert/delete (start-timer/stop-timer) whereas priority queue
 * based timers, such as java.util.concurrent.DelayQueue and java.util.Timer, have O(log n)
 * insert/delete cost.
 *
 * 一个bucket的时间范围是[u * (n -1), u * n) 左闭右开
 * 过期的任务不会插入到时间轮中，会被直接执行
 * 时间轮 插入/删除 timer（在bucket链表上插入、删除）的复杂度是O(1), 而DelayQueue 、Timer等通过优先队列的则需要O(log n)，因为取出、插入在优先队列中还需调整堆。
 * 当时间轮指向下个bucket，原来的bucket过期，然后复用插入末尾，用来指向新的区间[t + u * n, t + (n + 1) * u)，t为当前时间
 *
 * A major drawback of a simple timing wheel is that it assumes that a timer request is within
 * the time interval of n * u from the current time. If a timer request is out of this interval,
 * it is an overflow. A hierarchical timing wheel deals with such overflows. It is a hierarchically
 * organized timing wheels. The lowest level has the finest time resolution. As moving up the
 * hierarchy, time resolutions become coarser. If the resolution of a wheel at one level is u and
 * the size is n, the resolution of the next level should be n * u. At each level overflows are
 * delegated to the wheel in one level higher. When the wheel in the higher level ticks, it reinsert
 * timer tasks to the lower level. An overflow wheel can be created on-demand. When a bucket in an
 * overflow bucket expires, all tasks in it are reinserted into the timer recursively. The tasks
 * are then moved to the finer grain wheels or be executed. The insert (start-timer) cost is O(m)
 * where m is the number of wheels, which is usually very small compared to the number of requests
 * in the system, and the delete (stop-timer) cost is still O(1).
 *
 * 如果指定时间超过当前时间的范围（n*u），则需要新建一个overflow的时间轮，间隔为上一个时间轮的范围n*u
 * 当前上一级的时间轮的bucket到期时，里面的任务会重新尝试插入timer（整体），而实际上会执行（到期）or 插入低一级的时间轮。
 * 这时候插入低级轮的复杂度为O(m)，m为时间轮的数量，因为寻找合适的时间轮，而删除还是O(1)。
 * Example
 * Let's say that u is 1 and n is 3. If the start time is c,
 * then the buckets at different levels are:
 *
 * level    buckets
 * 1        [c,c]   [c+1,c+1]  [c+2,c+2]
 * 2        [c,c+2] [c+3,c+5]  [c+6,c+8]
 * 3        [c,c+8] [c+9,c+17] [c+18,c+26]
 *
 * The bucket expiration is at the time of bucket beginning.
 * So at time = c+1, buckets [c,c], [c,c+2] and [c,c+8] are expired.
 * Level 1's clock moves to c+1, and [c+3,c+3] is created.
 * Level 2 and level3's clock stay at c since their clocks move in unit of 3 and 9, respectively.
 * So, no new buckets are created in level 2 and 3.
 * 当timer指向下个bucket，上个bucket过期后，会补充一个bucket到最后（即新增一个区间）
 *
 * Note that bucket [c,c+2] in level 2 won't receive any task since that range is already covered in level 1.
 * The same is true for the bucket [c,c+8] in level 3 since its range is covered in level 2.
 * This is a bit wasteful, but simplifies the implementation.
 * 新增任务时，低级已经覆盖到的范围不会插入到高级的轮上
 *
 * 1        [c+1,c+1]  [c+2,c+2]  [c+3,c+3]
 * 2        [c,c+2]    [c+3,c+5]  [c+6,c+8]
 * 3        [c,c+8]    [c+9,c+17] [c+18,c+26]
 *
 * At time = c+2, [c+1,c+1] is newly expired.
 * Level 1 moves to c+2, and [c+4,c+4] is created,
 *
 * 1        [c+2,c+2]  [c+3,c+3]  [c+4,c+4]
 * 2        [c,c+2]    [c+3,c+5]  [c+6,c+8]
 * 3        [c,c+8]    [c+9,c+17] [c+18,c+18]
 *
 * At time = c+3, [c+2,c+2] is newly expired.
 * Level 2 moves to c+3, and [c+5,c+5] and [c+9,c+11] are created.
 * Level 3 stay at c.
 *
 * 1        [c+3,c+3]  [c+4,c+4]  [c+5,c+5]
 * 2        [c+3,c+5]  [c+6,c+8]  [c+9,c+11]
 * 3        [c,c+8]    [c+9,c+17] [c+8,c+11]
 *
 * The hierarchical timing wheels works especially well when operations are completed before they time out.
 * Even when everything times out, it still has advantageous when there are many items in the timer.
 * Its insert cost (including reinsert) and delete cost are O(m) and O(1), respectively while priority
 * queue based timers takes O(log N) for both insert and delete where N is the number of items in the queue.
 *
 * This class is not thread-safe. There should not be any add calls while advanceClock is executing.
 * It is caller's responsibility to enforce it. Simultaneous add calls are thread-safe.
 * startMs: 创建时间
  * currentTime：时间轮的初始时间
 */
@nonthreadsafe
private[timer] class TimingWheel(tickMs: Long, wheelSize: Int, startMs: Long, taskCounter: AtomicInteger, queue: DelayQueue[TimerTaskList]) {

  // 这个时间轮的总范围（总时长）
  private[this] val interval = tickMs * wheelSize
  /**
   * 时间轮的所有bucket，使用数组Array存放
   * 每个bucket对应一个TimerTaskList，里面存储结构是链表
   */
  private[this] val buckets = Array.tabulate[TimerTaskList](wheelSize) { _ => new TimerTaskList(taskCounter) }

  /**
   * 最近更新（跳转）时间轮的时间
   * 就是当前区间[t , t + (n + 1)), 的左区间
   * 类似zk的session按桶分组，使得每个区间左右两端都是tickMs的倍数
    */
  private[this] var currentTime = startMs - (startMs % tickMs) // rounding down to multiple of tickMs

  // overflowWheel can potentially be updated and read by two concurrent threads through add().
  // Therefore, it needs to be volatile due to the issue of Double-Checked Locking pattern with JVM
  @volatile private[this] var overflowWheel: TimingWheel = null

  private[this] def addOverflowWheel(): Unit = {
    synchronized {
      if (overflowWheel == null) {
        overflowWheel = new TimingWheel(
          tickMs = interval, // 当前间隔时间tickMs * 20
          wheelSize = wheelSize, // entry大小：20
          startMs = currentTime, // 父轮的开始时间(currentTime)在子轮的前面startMs - (startMs % tickMs)
          taskCounter = taskCounter,
          queue
        )
      }
    }
  }

  def add(timerTaskEntry: TimerTaskEntry): Boolean = {
    val expiration = timerTaskEntry.expirationMs

    if (timerTaskEntry.cancelled) {
      // Cancelled
      false
    } else if (expiration < currentTime + tickMs) { // 可能机器处理其他东西，有延迟，导致已达到过期时间
      // Already expired
      false
      /**
       * 指定时间 在 当前时间轮的范围（当前时间+20*间隔），放到当前轮中
       */
    } else if (expiration < currentTime + interval) {
      // Put in its own bucket

      /**
       * 放入bucket位置：
       * (到期时间/ 当前轮的间隔tickMs) % 20
       * 其中 bucket号*间隔 就是 实际过期时间
       */
      val virtualId = expiration / tickMs
      val bucket = buckets((virtualId % wheelSize.toLong).toInt)
      bucket.add(timerTaskEntry)

      // Set the bucket expiration time
      /**
       * 设置bucket过期时间（准确时间）：bucket号*间隔, 然后把bucket放入DelayQueue中
       * setExpiration: 如果与bucket（对象）当前的过期时间相同, 返回false，不会放入
       *
       * 结论：往DelayQueue放入当前过期时间的bucket, 如果当前过期时间bucket已经放入过，不用再放入
       * --- 一个过期时间的bucket只会放入延时队列一次
       */
      if (bucket.setExpiration(virtualId * tickMs)) {
        // The bucket needs to be enqueued because it was an expired bucket
        // We only need to enqueue the bucket when its expiration time has changed, i.e. the wheel has advanced
        // and the previous buckets gets reused; further calls to set the expiration within the same wheel cycle
        // will pass in the same value and hence return false, thus the bucket with the same expiration will not
        // be enqueued multiple times.
        queue.offer(bucket)
      }
      true
    } else {
      /**
       * 指定时间 超过 当前时间轮的范围（当前时间+20*间隔），要放到范围更大的时间轮中
       */
      // Out of the interval. Put it into the parent timer
      if (overflowWheel == null) addOverflowWheel()
      overflowWheel.add(timerTaskEntry)
    }
  }

  // Try to advance the clock
  /**
   * 更新当前时间，等于timer运转到下一个ms
   * @param timeMs
   */
  def advanceClock(timeMs: Long): Unit = {
    // timeMs >= 当前时间+间隔 ，就是这个时间 超过现有的第一个时间点，所以把当
    /**
     * 判断是为了避免错误，正常进入肯定超过时间轮的当前时间+间隔 ，代表时间轮当前的bucket（）过期，可以跳转下一个bucket
     */
    if (timeMs >= currentTime + tickMs) {
      currentTime = timeMs - (timeMs % tickMs)

      // 如果有上级的时间轮，也需要更新对应的开始时间
      // Try to advance the clock of the overflow wheel if present
      if (overflowWheel != null) overflowWheel.advanceClock(currentTime)
    }
  }
}
