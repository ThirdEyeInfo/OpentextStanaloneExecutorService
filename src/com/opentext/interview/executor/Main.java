package com.opentext.interview.executor;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import com.opentext.interview.executor.constant.TaskType;
import com.opentext.interview.executor.records.Task;
import com.opentext.interview.executor.records.TaskGroup;
import com.opentext.interview.executor.service.TaskExecutorImpl;

public class Main {

	/**
	 * For sake of simplicity, I have moved all classes,interfaces, and records to
	 * their corresponding package structure.
	 */
	public static final BlockingQueue<String> TASK_QUEUE = new LinkedBlockingQueue<>();

	public static void main(String[] args) throws InterruptedException, ExecutionException {

		final TaskExecutorImpl executorService = new TaskExecutorImpl();

		final TaskGroup groupWrite = new TaskGroup(UUID.randomUUID());
		final TaskGroup groupRead = new TaskGroup(UUID.randomUUID());

		final UUID task1UUID = UUID.randomUUID();
		Task<String> task1 = new Task<>(task1UUID, groupWrite, TaskType.WRITE, () -> {

			//Thread.sleep(500);
			final String writer = "Written by 1: " + task1UUID + " Write Time " + Instant.now();
			
			TASK_QUEUE.add(writer);
			return writer;
		});

		final UUID task2UUID = UUID.randomUUID();
		Task<String> task2 = new Task<>(task2UUID, groupRead, TaskType.READ, () -> {

			return !TASK_QUEUE.isEmpty() ? "Read by 1 and " + TASK_QUEUE.remove() + " vs Read Time " + Instant.now(): "Read by 1: " + task2UUID + " Read Time" + Instant.now();
		});
		
		final UUID task3UUID = UUID.randomUUID();
		Task<String> task3 = new Task<>(task3UUID, groupWrite, TaskType.WRITE, () -> {
			
			final String writer = "Written by 2: " + task3UUID + " Write Time " + Instant.now();
			
			TASK_QUEUE.add(writer);
			return writer;
		});

		final UUID task4UUID = UUID.randomUUID();
		Task<String> task4 = new Task<>(task4UUID, groupRead, TaskType.READ, () -> {

			return !TASK_QUEUE.isEmpty() ? "Read by 2 and " + TASK_QUEUE.remove() + " vs Read Time " + Instant.now(): "Read by 2: " + task4UUID + " Read Time" + Instant.now();
		});

		/**
		 * 1.	Tasks can be submitted concurrently. Task submission should not block the submitter.
		 * 
		 * Non blocking code to submit concurrently. 
		 */
		Future<String> future1 = executorService.submitTask(task1); // WRITER THREAD
		Future<String> future2 = executorService.submitTask(task2); // READER THREAD
		Future<String> future3 = executorService.submitTask(task3); // WRITER THREAD
		Future<String> future4 = executorService.submitTask(task4); // READER THREAD

		/**
		 * 3.	Once task is finished, its results can be retrieved from the Future received during task submission.
		 */
		System.out.println(future1.get());
		System.out.println(future2.get());
		System.out.println(future3.get());
		System.out.println(future4.get());

		executorService.getExecutorService().shutdown();

	}

}
