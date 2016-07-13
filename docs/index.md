# Thor 实时采集文档

Thor 是基于 kafka-connect standalone 模式实现的对日志文件的实时采集系统

使用说明：

```bash
    mvn assembly:assembly
```

将编译打成的 tar 包部署到采集的服务器

部署采集：

```bash
    sudo su - 
    mkdir /data/thor
    tar zxf thor-0.1.0-bin.tar.gz -C /data/thor
    cd /data/thor/thor-0.1.0
    
    # 部署 adwiser 采集
    bin/thor.sh config/adwiser/worker-adwiser.properties config/adwiser/adwiser-browse.properties config/adwiser/adwiser-goods.properties config/adwiser/adwiser-order.properties config/adwiser/adwiser-event.properties &
    
    # 部署 bs web 采集
    bin/thor.sh config/bs/worker-bs-web.properties config/bs/bs-monitor-web-a.properties config/bs/bs-monitor-web-b.properties &
    
    # 部署 bs mobile 采集
    bin/thor.sh config/bs/worker-bs-mobile.properties config/bs/bs-monitor-click-mobile-a.properties config/bs/bs-monitor-click-mobile-b.properties config/bs/bs-monitor-imp-mobile-a.properties config/bs/bs-monitor-imp-mobile-b.properties &
    
    # 部署 rtb 采集
    # 发往北京 Kafka 的北京日志服务器启动命令
    bin/thor.sh config/rtb/worker-bj-rtb.properties config/rtb/rtb-request-pc.properties config/rtb/rtb-camp-pc.properties config/rtb/rtb-request-mobile.properties config/rtb/rtb-camp-mobile.properties &
    # 发往北京 Kafka 的杭州日志服务器启动命令
    bin/thor.sh config/rtb/worker-bj-rtb.properties config/rtb/rtb-camp-mobile.properties &
    
    # 发往杭州 Kafka 的杭州日志服务器启动命令
    bin/thor.sh config/rtb/worker-hz-rtb.properties config/rtb/rtb-request-pc.properties config/rtb/rtb-camp-pc.properties config/rtb/rtb-request-mobile.properties &  
```

实时采集监控：
worker.properties 中配置了 rest.port 端口，每一个进程有一个 rest server，默认端口为 8083
采集程序配置的端口为: 
 1. Adwiser: 8183
 2. BS: 8184
 4. 发往北京 Kafka 的 RTB: 8185
 5. 发往杭州 Kafka 的 RTB: 8285 (暂时未采集)

rest api：

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
5. 通知某一个 connector 采集
```rest
PUT /connectors/${connector-name}/pause
Host: connect.example.com
```
6. 开始一个在停止状态的 connector
```rest
PUT /connectors/${connector-name}/resume
Host: connect.example.com
```
7. 重启一个 connector 
```rest
POST /connectors/${connector-name}/restart
Host: connect.example.com
```