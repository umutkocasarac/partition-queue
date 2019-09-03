package partition.queue;

import static org.junit.Assert.assertTrue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import partition.queue.PartitionQueue.Callback;

public class PartitionQueueTest {
	private static final Logger logger = LoggerFactory.getLogger(PartitionQueueTest.class);

	@Test
	public void testCreation() {
		int size = 4;
		PartitionQueue queue = new PartitionQueue(size);
		Assert.assertEquals(queue.getConsumerList().size(), size);
	}

	@Test
	public void testPartitionAssignment() {
		int size = RandomUtils.nextInt(1, 20);
		PartitionQueue queue = new PartitionQueue(size);
		String partitionKey = RandomStringUtils.random(5);
		Consumer c = queue.getConsumer(partitionKey);
		Consumer c2 = queue.getConsumer(partitionKey);
		Assert.assertEquals(c, c2);
	}

	@Test
	public void testQueue() throws InterruptedException {
		int iteration = 10;
		PartitionQueue queue = new PartitionQueue(4);
		for (int i = 1; i <= iteration; i += 1) {
			queue.add(String.valueOf(i), new PartitionThread(i));
		}
	}

	@Test
	public void testQueueCallback() throws InterruptedException {
		int iteration = 10;
		PartitionQueue queue = new PartitionQueue(4);
		CountDownLatch countDownLatch = new CountDownLatch(iteration * 2);
		for (int i = 1; i <= iteration; i += 1) {
			queue.add(String.valueOf(i), new PartitionThread(i), new CallbackImp(i, countDownLatch));
		}
		for (int i = 1; i <= iteration; i += 1) {
			queue.add(String.valueOf(i), new PartitionThread(i), new CallbackImp(i, countDownLatch));
		}
		assertTrue(countDownLatch.await(1, TimeUnit.SECONDS));
	}

	static class PartitionThread extends Thread {
		int i;

		PartitionThread(int i) {
			this.i = i;
		}

		public void run() {
			logger.info(String.valueOf(i));
		}

	}

	private static class CallbackImp implements Callback {
		int i;
		CountDownLatch countDownLatch;

		CallbackImp(int i, CountDownLatch countDownLatch) {
			this.i = i;
			this.countDownLatch = countDownLatch;
		}

		@Override
		public void onComplete() {
			logger.info("Oncomplete is working {}", i);
			countDownLatch.countDown();

		}

		@Override
		public void onCancel() {
			logger.info("onCancel is working ()", i);
		}

	}
}
