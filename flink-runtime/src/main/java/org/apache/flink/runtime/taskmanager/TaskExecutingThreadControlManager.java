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

import java.io.*;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Properties;
import java.util.concurrent.*;

public class TaskExecutingThreadControlManager {
	private static final Logger LOG = LoggerFactory.getLogger(TaskExecutingThreadControlManager.class);

	private final static int RUNNING_THREAD_NUM = 4;
	private final static int EXTRA_THREAD_NUM = 1;
	private final static int INIT_SCHEDULE_DELAY = 20000;
	private final static int SCHEDULE_DELAY = 300;
	private int runningThreadNum = RUNNING_THREAD_NUM;
	private int extraThreadNum = EXTRA_THREAD_NUM;
	private int initSchedulingDelay = INIT_SCHEDULE_DELAY;
	private int schedulingDelay = SCHEDULE_DELAY;
	//
	private Properties threadControlProp;

	private TaskSlotTable taskSlotTable;

	private boolean hasTaskAdded = false;
	private final String synControl = "";

	private LinkedList<ExecutionAttemptID> waitingTasks;
	private LinkedList<ExecutionAttemptID> runningTasks;
//	private BlockingQueue<ExecutionAttemptID> inQueueTasks;

	// control thread state periodically
	private ScheduledExecutorService threadPool;

	public TaskExecutingThreadControlManager(TaskSlotTable taskSlotTable) {
		this.taskSlotTable = taskSlotTable;

		waitingTasks = new LinkedList<>();
		runningTasks = new LinkedList<>();
//		inQueueTasks = new LinkedBlockingQueue<>();

		threadPool = Executors.newSingleThreadScheduledExecutor();

		try {
			InputStream in = new BufferedInputStream(new FileInputStream(
				new File("/usr/local/etc/flink-resource/conf/task-control.properties")));
			threadControlProp = new Properties();
			threadControlProp.load(in);
			runningThreadNum = Integer.parseInt(threadControlProp.getProperty("RunningThreadNum"));
			extraThreadNum = Integer.parseInt(threadControlProp.getProperty("ExtraThreadNum"));
			initSchedulingDelay = Integer.parseInt(threadControlProp.getProperty("InitSchedulingDelay"));
			schedulingDelay = Integer.parseInt(threadControlProp.getProperty("SchedulingDelay"));
			LOG.info("read from properties, runningThreadNum:{}, extraThreadNum:{}, initDelay:{}, delay:{}", runningThreadNum, extraThreadNum, initSchedulingDelay, schedulingDelay);
		} catch (FileNotFoundException e) {
			LOG.warn("properties not found");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void onStart() {
		threadPool.scheduleWithFixedDelay(new TaskThreadControlExecutor(), initSchedulingDelay, schedulingDelay, TimeUnit.MILLISECONDS);
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

				long now = System.currentTimeMillis();
				Task tempTask = null;

				// add all threads into waiting
				for (Iterator<ExecutionAttemptID> iterator = runningTasks.iterator(); iterator.hasNext();) {
					tempTask = taskSlotTable.getTask(iterator.next());
					if (null != tempTask) {
						iterator.remove();
						waitingTasks.add(tempTask.getExecutionId());
					} else {
						LOG.info("runningTask is null");
						iterator.remove();
					}
				}

				// add #THREAD_NUM longest threads into running & resume these tasks
				for (int i = 0; i < runningThreadNum; i++) {
					//find longest in waiting
					Task longestTask = null;
					int longestLength = -1;

					for (Iterator<ExecutionAttemptID> iterator = waitingTasks.iterator(); iterator.hasNext();) {
						tempTask = taskSlotTable.getTask(iterator.next());
						if (null != tempTask) {
							int length = tempTask.getBufferInputQueueLength();
							LOG.info("waitingTask {}, queue {}", tempTask, length);
							if (length > longestLength) {
								longestLength = length;
								longestTask = tempTask;
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

				// add extra running tasks
				for (int i = 0; i < extraThreadNum; i++) {
					tempTask = taskSlotTable.getTask(waitingTasks.removeFirst());
					if (null != waitingTasks) {
						runningTasks.add(tempTask.getExecutionId());
						resumeTask(tempTask);
					}
				}

				// suspend others
				for (Iterator<ExecutionAttemptID> iterator = waitingTasks.iterator(); iterator.hasNext();) {
					tempTask = taskSlotTable.getTask(iterator.next());
					if (null != tempTask) {
						suspendTask(tempTask);
					}
				}

				LOG.info("finish adjusting running task using {}ms", System.currentTimeMillis() - now);
				LOG.info("++++++++++++++++++++++++++++++++++++++++++++++++++");
			}
//			LOG.info("adjust Running Task free lock");
		}

		private void suspendTask(Task task) {
			task.setTaskSuspend(true);
//			LOG.info("finish suspendTask {}, now running num:{}, waiting num:{}",task, runningTasks.size(), waitingTasks.size());
		}

		private void resumeTask(Task task) {
			task.setTaskSuspend(false);
//			LOG.info("finish resumeTask {}, now running num:{}, waiting num:{}",task, runningTasks.size(), waitingTasks.size());
		}
	}

}

