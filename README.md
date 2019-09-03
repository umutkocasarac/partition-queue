# Partition Queue

Simple partitioned queue. Items on the queue always be ordered by the key. Concurrency could be incremented via using partitionCount parameter


# Sample Usage
```java
PartitionQueue queue = new PartitionQueue(partitionSize);
queue.add(partitionKey, thread, new Callback() {

	@Override
	public void onComplete() {
		
	}
	
	@Override
	public void onCancel() {
		
	}
});
```