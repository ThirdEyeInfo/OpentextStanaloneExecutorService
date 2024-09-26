package com.opentext.interview.executor.service;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;

import com.opentext.interview.executor.constant.TaskType;
import com.opentext.interview.executor.records.Task;

public class TaskExecutorImpl implements TaskExecutor {

	/**
	 * 4. The order of tasks must be preserved. 
	 *            o The first task submitted must be the first task started.
	 *            o	The task result should be available as soon as possible after the task completes.
	 * 
	 * By setting ExecutorService with FixedThreadPool size of 2 and by creating 2 Semaphore instance for 
	 * each TaskType, we can make sure above. However, if sequence of execution is not an issue we can
	 * set to higher value. We can also create only one Semaphore instance with max value 1 to run all 
	 * type of task sequentially. But doing so will hamper the scope of creation of READ and WRITE groups. 
	 */
	
	/**
	 * 2. Tasks are executed asynchronously and concurrently. Maximum allowed
	 * concurrency may be restricted.
	 * 
	 * Maximum allowed concurrency has been restricted to value = 2. So, that this
	 * at a moment only two threads (one WRITE thread + one READ thread) should run.
	 */
	private final ExecutorService executor = Executors.newFixedThreadPool(2);

	private final Map<UUID, Semaphore> taskGroupLocks = new ConcurrentHashMap<>();

	@Override
	public <T> Future<T> submitTask(Task<T> task) {

		/**
		 * 5.	Tasks sharing the same TaskGroup must not run concurrently.
		 * 
		 * if (task.taskType() == TaskType.WRITE) at line 53 will only run WRITE task else READ.
		 */
		final Semaphore semaphore = taskGroupLocks.computeIfAbsent(task.taskGroup().groupUUID(),
				value -> new Semaphore(1));

		System.out.println("For Random GroupID : " + task.taskUUID() + " has Semaphore value "
				+ semaphore.availablePermits() + " before lock acquire to " + task.taskType() + ".");

		// If TaskType is WRITE operation
		if (task.taskType() == TaskType.WRITE) {

			return completeTask(semaphore, task);
		}

		// Else READ operation
		try {
			/**
			 * Thread sleep set to 1 seconds to let writer thread to complete write process.
			 * We can also uncomment above three lines to do same job for parallel
			 * processing.
			 */
			System.err.println("Read thread is on sleep to let write process complete their task");
			Thread.sleep(500);
		} catch (InterruptedException e) {
			System.out.println(e.getMessage());
		}
		return completeTask(semaphore, task);
	}

	private <T> Future<T> completeTask(final Semaphore semaphore, final Task<T> task) {
		try {
			semaphore.acquire();

			System.out.println("For Random GroupID : " + task.taskUUID() + " has Semaphore value "
					+ semaphore.availablePermits() + " after lock acquire to " + task.taskType() + ".");

			return executor.submit(task.taskAction());
		} catch (InterruptedException e) {
			System.out.println(e.getMessage());
		} finally {
			semaphore.release();
			System.out.println("For Random GroupID : " + task.taskUUID() + " has Semaphore value "
					+ semaphore.availablePermits() + " after lock release to " + task.taskType() + ".");
		}

		throw new RuntimeException("Oops! Something went wrong.");
	}

	public ExecutorService getExecutorService() {
		return executor;
	}

}
