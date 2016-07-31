# Thor 实时采集文档

Thor 是基于 kafka-connect (0.10.0.0) 实现的对日志文件的实时采集系统

## 主要应用场景

1. 一个日志服务器有多种日志，需要采集到多个 kafka topic，Thor 可以只启动一个进程完成采集
2. 主要针对日志文件是以一定时间周期滚动的形式 （目前不支持日志文件名不变的切割方式）
3. 可以对行定制格式化（目前是 java，后期计划支持脚步形式）

## 使用方式

### 依赖：

+ jdk 1.8 or later

### 编译

```bash
    git clone https://github.com/hk-Lei/thor.git
    cd thor
    mvn assembly:assembly
```

将编译打成的 tar 包部署到采集的服务器

### 启动

启动采集程序：

```bash
    cd ${thor.path}
    # worker-example.properties 是采集管理进程配置文件
    # connect-example.properties 是采集线程配置文件
    bin/thor.sh config/worker-example.properties config/connect-example.properties &
```
    
### Rest API

1. 查看某个服务器的采集进程的 connectors：
```rest
GET /connectors
```
2. 查看采集进程中的某个 connector
```rest
GET /connectorts/${connector-name}
```
3. 获取某个 connector 的配置信息
```rest
GET /connectors/${connector-name}/config
```
4. 获取某个 connector 的状态
```rest
GET /connectors/${connector-name}/status
```
5. 停止某一个 connector 采集
```rest
PUT /connectors/${connector-name}/pause
Host: connect.example.com
```
6. 启动一个在停止状态的 connector
```rest
PUT /connectors/${connector-name}/resume
Host: connect.example.com
```
7. 重启一个 connector 
```rest
POST /connectors/${connector-name}/restart
Host: connect.example.com
```

## Note

经阅读 Kafka Connect 的 `org.apache.kafka.connect.runtime.WorkerSourceTask` 类的源码，发现其 `outstandingMessages （IdentityHashMap<ProducerRecord<byte[], byte[]>, ProducerRecord<byte[], byte[]>>）` 的大小不受限制，当某些原因导致采集日志的写入速度大于发往 kafka 的速度时，其 size 会剧增可能会导致 offset flush 失败或者内存溢出等异常，因为我加入了以下代码做了简单的控制：

```java
    ...
    // 加入类变量
    // 当发送到 Kafka 的速度（即删除 outstandingMessages 的速度）远小于写入 outstandingMessages 速度时，
    // outstandingMessages 的大小会累加，当其大小到一定值时，此线程等待一会，使 outstandingMessages 被删除一些
    private static final long SEND_WAIT_MS = 100;
    private static final long OUTSTANDING_MESSAGES_THRESHOLD = 10000;
    ...
    // 在 execute() 方法中的 while 循环末尾加入以下代码：
    while (outstandingMessages.size() > OUTSTANDING_MESSAGES_THRESHOLD) {
        log.info("About {} outstandingMessages records to Kafka, stopRequestedLatch.await({}ms)", outstandingMessages.size(), SEND_WAIT_MS);
        stopRequestedLatch.await(SEND_WAIT_MS, TimeUnit.MILLISECONDS);
    }
```

修改后的 connect-runtime 项目已经编译到 libs 下 `connect-runtime-0.10.0.0.jar`，使用之前的编译命令会自动打包到 thor 的安装包中，无需再做其他引入操作。如果有其他更好的修改意见，可邮件至 `moxingxing.lei@gmail.com`。 

## Maintainer

* hk-Lei ([@hk-Lei](moxingxing.lei@gmail.com))