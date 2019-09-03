# Partition Queue

Simple partitioned queue. Items on the queue always be ordered by the key. Concurrency could be incremented via using partitionCount parameter


# Sample Usage
```java
PartitionQueue queue = new PartitionQueue(partitionSize);
queue.add(String.valueOf(i), new PartitionThread(i), new Callback() {

	@Override
	public void onComplete() {
		
	}
	
	@Override
	public void onCancel() {
		
	}
});
```