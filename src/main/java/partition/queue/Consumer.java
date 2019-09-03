package partition.queue;

import java.util.concurrent.BlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import partition.queue.PartitionQueue.CallbackThread;

class Consumer extends Thread {
	private static final Logger logger = LoggerFactory.getLogger(Consumer.class);

	private final BlockingQueue<CallbackThread> blockingQueue;

	Consumer(BlockingQueue<CallbackThread> blockingQueue) {
		this.blockingQueue = blockingQueue;
	}

	@Override
	public void run() {
		try {
			while (true) {
				CallbackThread callbackThread = blockingQueue.take();
				callbackThread.getThread().setName(this.getName());
				callbackThread.getThread().start();
				callbackThread.getThread().join();
				if (callbackThread.getCallback() != null) {
					callbackThread.getCallback().onComplete();
				}
			}
		} catch (InterruptedException ex) {
			logger.error("Consumer thread interrupted {}", this.getName());
			Thread.currentThread().interrupt();
		}
	}

	void shutdown() {
		logger.info("Shutdown initiated for {}", this.getName());
		logger.info("Size of the queue {}", blockingQueue.size());
		blockingQueue.forEach(t -> {
			if (t.getCallback() != null)
				t.getCallback().onCancel();
		});
	}

	BlockingQueue<CallbackThread> getBlockingQueue() {
		return blockingQueue;
	}
}
