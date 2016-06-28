# Thor 实时采集文档

Thor 是基于 kafka-connect (0.10.0.0) standalone 模式实现的对日志文件的实时采集系统

## 主要应用场景

1. 一个日志服务器有多种日志，需要采集到多个 kafka topic，Thor 可以只启动一个进程完成采集
2. 主要针对日志文件是以一定时间周期滚动的形式 （目前不支持日志文件名不变的切割方式）
3. 可以对行定制格式化（目前是 java，后期计划支持脚步形式）

## 使用方式


## Maintainer

* hk-Lei ([@hk-Lei](moxingxing.lei@gmail.com))