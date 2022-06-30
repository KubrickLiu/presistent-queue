![Apache License, Version 2.0, January 2004](https://img.shields.io/github/license/apache/maven.svg?label=License)
[![Maven Central](https://img.shields.io/maven-central/v/org.apache.maven/apache-maven.svg?label=Maven%20Central)](https://search.maven.org/artifact/org.apache.maven/apache-maven)

# presistent-queue
Persistent message queue. Messages are stored in the disk. Messages by cursor are supported

# What this is
persistent-message-queue is a queue SDK that stores messages on disk.

### Function point
* Messages are stored in the disk to ensure that the service will not stackoverflow in memory due to too many messages;
  * 消息存储在磁盘中，可以保证服务不会因消息过多导致内存溢出;
* Messages are not one-time consumption. Messages can be subscribed again by cursor;
  * 消息不是一次性消费，消息可以按 cursor 再次订阅;
* Service restart will not lose messages;
  * 服务重新启动不会丢失消息;
* After the service is restarted, it can automatically continue to execute from the last address written or read;
  * 服务重新启动后可以自动从上次写入或读取的地址继续执行;
* Thanks to Kafka logsegment;
  * 致谢 Kafka LogSegment;
## Quick-start
Maven:

```maven
<dependency>
    <groupId>io.github.kubrickLiu</groupId>
    <artifactId>persistent-queue</artifactId>
    <version>1.0.1</version>
</dependency>
```

### Example Code :

### 1. Single file queue ;
#### produce message:
```java
File file = new File("${file path}");

// create a single file persistent file -- FileRecords
FileRecords fileRecords = new FileRecords(file);

// mock id and message
int id = idGenerator.getAndIncrement();
String message = "msg-" + id;

// build a Record 
Record record = new Record(id, message.getBytes());

// append message to persistent queue
fileRecords.appendOne(record);
```

#### consume message:
```java
// get iterator from FileRecords
Iterator<Record> iterator = fileRecords.iterator();

// consume record from persistent queue
while (iterator.hasNext()) {
    Record result = iterator.next();
    System.out.println(result.getId() + " : " + new String(result.getBytes()));
}
```