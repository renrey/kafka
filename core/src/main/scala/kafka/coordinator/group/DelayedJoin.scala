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

package kafka.coordinator.group

import kafka.server.{DelayedOperation, DelayedOperationPurgatory, GroupKey}

import scala.math.{max, min}

/**
 * Delayed rebalance operations that are added to the purgatory when group is preparing for rebalance
 *
 * Whenever a join-group request is received, check if all known group members have requested
 * to re-join the group; if yes, complete this operation to proceed rebalance.
 *
 * When the operation has expired, any known members that have not requested to re-join
 * the group are marked as failed, and complete this operation to proceed rebalance with
 * the rest of the group.
 */
private[group] class DelayedJoin(coordinator: GroupCoordinator,
                                 group: GroupMetadata,
                                 rebalanceTimeout: Long) extends DelayedOperation(rebalanceTimeout, Some(group.lock)) {

  // 先通过tryCompleteJoin 校验是否可完成，可完成则强制forceComplete（调用onComplete）
  override def tryComplete(): Boolean = coordinator.tryCompleteJoin(group, forceComplete _)
  override def onExpiration(): Unit = {
    // try to complete delayed actions introduced by coordinator.onCompleteJoin
    tryToCompleteDelayedAction()
  }
  // 完成join
  override def onComplete(): Unit = coordinator.onCompleteJoin(group)

  // TODO: remove this ugly chain after we move the action queue to handler thread
  private def tryToCompleteDelayedAction(): Unit = coordinator.groupManager.replicaManager.tryCompleteActions()
}

/**
  * Delayed rebalance operation that is added to the purgatory when a group is transitioning from
  * Empty to PreparingRebalance
  *
  * When onComplete is triggered we check if any new members have been added and if there is still time remaining
  * before the rebalance timeout. If both are true we then schedule a further delay. Otherwise we complete the
  * rebalance.
 *
 * 初始的跟平常的最大不同是，会在指定范围内（6s）一直接收join请求，直到时间结束才去完成，而普通只要在范围有join请求，就会去完成
  */
private[group] class InitialDelayedJoin(coordinator: GroupCoordinator,
                                        purgatory: DelayedOperationPurgatory[DelayedJoin],
                                        group: GroupMetadata,
                                        configuredRebalanceDelay: Int,
                                        delayMs: Int,
                                        remainingMs: Int) extends DelayedJoin(coordinator, group, delayMs) {

  override def tryComplete(): Boolean = false

  override def onComplete(): Unit = {
    group.inLock {
      /**
       * 就是这个第一次PreparingRebalance到期前，有其他consumer发送join
       * remainingMs: 组内最大rebalanceTimeoutMs - broker上的groupInitialRebalanceDelayMs(group.min.session.timeout.ms，默认6s)
       */
      if (group.newMemberAdded && remainingMs != 0) {
        group.newMemberAdded = false
        val delay = min(configuredRebalanceDelay, remainingMs)
        // 一只循环直到总共到达6s
        val remaining = max(remainingMs - delayMs, 0)
        /**
         * 其实就继续延时等待其他consumer发送join，一直这样循环，直到过去真实需要等待的时间（configuredRebalanceDelay, 6s）
         */
        purgatory.tryCompleteElseWatch(new InitialDelayedJoin(coordinator,
          purgatory,
          group,
          configuredRebalanceDelay,
          delay,
          remaining
        ), Seq(GroupKey(group.groupId)))
      // 已经到期了（已达到6s），直接完成
      } else
        super.onComplete()
    }
  }

}
