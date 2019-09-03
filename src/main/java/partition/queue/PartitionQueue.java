package partition.queue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PartitionQueue {
	private static final Logger logger = LoggerFactory.getLogger(PartitionQueue.class);

	private final int partitionCount;
	private final List<Consumer> consumerList;

	/**
	 * Initiate the partition queue
	 * 
	 * @param partitionCount Total number of the partition
	 */
	public PartitionQueue(int partitionCount) {
		if (partitionCount <= 0) {
			throw new IllegalArgumentException("Partition count should be bigger than 0");
		}
		this.partitionCount = partitionCount;
		this.consumerList = new ArrayList<>(partitionCount);
		IntStream.range(0, partitionCount).forEach(i -> {
			BlockingQueue<CallbackThread> queue = new LinkedBlockingDeque<>();
			Consumer consumer = new Consumer(queue);
			consumer.start();
			consumer.setName("Consumer-id-" + i);
			consumerList.add(consumer);
		});
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				consumerList.forEach(Consumer::shutdown);
				logger.error("Shutdown Hook is running !");
			}
		});
	}

	/**
	 * Add item to queue
	 * 
	 * @param partitionKey All messages with the same key will be consumed by order
	 * @param thread Thread which will be executed by queue
	 */
	public void add(String partitionKey, Thread thread) {
		getConsumer(partitionKey).getBlockingQueue().add(new CallbackThread(thread, null));
	}

	/**
	 * Add item to queue
	 * 
	 * @param partitionKey All messages with the same key will be consumed by order
	 * @param thread Thread which will be executed by queue
	 * @param callback Callback will be executed after execution completed
	 */
	public void add(String partitionKey, Thread thread, Callback callback) {
		getConsumer(partitionKey).getBlockingQueue().add(new CallbackThread(thread, callback));
	}

	Consumer getConsumer(String partitionKey) {
		int h = partitionKey.hashCode();
		h = h ^ (h >>> 16);
		return consumerList.get(h % partitionCount);
	}



	static class CallbackThread {

		private final Callback callback;
		private final Thread thread;

		CallbackThread(Thread thread, Callback callback) {
			this.thread = thread;
			this.callback = callback;
		}

		public Callback getCallback() {
			return callback;
		}

		public Thread getThread() {
			return thread;
		}
	}

	public interface Callback {
		public void onComplete();

		public void onCancel();
	}

	public int getPartitionCount() {
		return partitionCount;
	}

	public List<Consumer> getConsumerList() {
		return Collections.unmodifiableList(consumerList);
	}

}
