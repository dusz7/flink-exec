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

package org.apache.flink.runtime.taskmanager;

import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.taskexecutor.slot.TaskSlotTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;

public class TaskExecutingThreadControlManager {
	private static final Logger LOG = LoggerFactory.getLogger(TaskExecutingThreadControlManager.class);

	private final static int THREAD_NUM = 4;
	private final static int INITSCHEDULE_DELAY = 20000;
	private final static int SCHEDULE_DELAY = 300;

	private TaskSlotTable taskSlotTable;

	private boolean hasTaskAdded = false;
	private final String synControl = "";

	private List<ExecutionAttemptID> waitingTasks;
	private List<ExecutionAttemptID> runningTasks;
//	private BlockingQueue<ExecutionAttemptID> inQueueTasks;

	// control thread state periodically
	private ScheduledExecutorService threadPool;

	public TaskExecutingThreadControlManager(TaskSlotTable taskSlotTable) {
		this.taskSlotTable = taskSlotTable;

		waitingTasks = new LinkedList<>();
		runningTasks = new LinkedList<>();
//		inQueueTasks = new LinkedBlockingQueue<>();

		threadPool = Executors.newSingleThreadScheduledExecutor();
	}

	private void onStart() {
		threadPool.scheduleWithFixedDelay(new TaskThreadControlExecutor(), INITSCHEDULE_DELAY, SCHEDULE_DELAY, TimeUnit.MILLISECONDS);
	}

	public void addTask(ExecutionAttemptID id) {
//		LOG.info("add task waiting lock");
		synchronized (synControl) {
//			LOG.info("add task get lock");
			Task task = taskSlotTable.getTask(id);
			if (null != task) {
				LOG.info("add task {},  id:{}", task, id);
//				waitingTasks.add(id);
				runningTasks.add(id);
//				task.setSuspend(true);

				if (!hasTaskAdded) {
					hasTaskAdded = true;
					onStart();
				}
			}
		}
//		LOG.info("add task free lock");
	}

	private class TaskThreadControlExecutor implements Runnable {

		@Override
		public void run() {
			adjustRunningTask();
		}

		private void adjustRunningTask() {
//			LOG.info("adjust Running Task... waiting lock");
			synchronized (synControl) {
				LOG.info("adjust Running Task get lock, runningTask num:{}, waitingTask num:{}",runningTasks.size(), waitingTasks.size());
				// add all threads into waiting
				long now = System.currentTimeMillis();
				if (runningTasks.size() > 0) {
					for (Iterator<ExecutionAttemptID> iterator = runningTasks.iterator(); iterator.hasNext();) {
						Task runningTask = taskSlotTable.getTask(iterator.next());
						if (null != runningTask) {
							iterator.remove();
							waitingTasks.add(runningTask.getExecutionId());
						} else {
							LOG.info("runningTask is null");
							iterator.remove();
						}
					}
				}

				// get longest from all threads and resume
				for (int i = 0; i < THREAD_NUM; i++) {
					//find longest in waiting
					Task longestTask = null;
					int longestLength = -1;

					for (Iterator<ExecutionAttemptID> iterator = waitingTasks.iterator(); iterator.hasNext();) {
						Task waitingTask = taskSlotTable.getTask(iterator.next());
						if (waitingTask != null) {
							int length = waitingTask.getBufferInputQueueLength();
							LOG.info("waitingTask {}, queue {}", waitingTask, length);
							if (length > longestLength) {
								longestLength = length;
								longestTask = waitingTask;
							}
						} else {
							LOG.info("waitingTask is null");
							// task is canceled
							iterator.remove();
						}
					}
					if (longestTask != null) {
						LOG.info("task {} will be resuming...", longestTask);
						runningTasks.add(longestTask.getExecutionId());
						waitingTasks.remove(longestTask.getExecutionId());
						resumeTask(longestTask);
					} else {
						// has no more task
						LOG.info("has no more task in waitingTasks");
						break;
					}
				}

				// suspend others
				for (Iterator<ExecutionAttemptID> iterator = waitingTasks.iterator(); iterator.hasNext();) {
					Task realWaitingTask = taskSlotTable.getTask(iterator.next());
					if (realWaitingTask != null) {
						suspendTask(realWaitingTask);
					}
				}

				LOG.info("finish adjusting running task using {}ms", System.currentTimeMillis() - now);
				LOG.info("	--------------------------------------");
			}
//			LOG.info("adjust Running Task free lock");
		}

		private void suspendTask(Task task) {
			task.setSuspend(true);
//			LOG.info("finish suspendTask {}, now running num:{}, waiting num:{}",task, runningTasks.size(), waitingTasks.size());
		}

		private void resumeTask(Task task) {
			task.setSuspend(false);
//			LOG.info("finish resumeTask {}, now running num:{}, waiting num:{}",task, runningTasks.size(), waitingTasks.size());
		}
	}

}

