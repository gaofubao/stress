# stress - 数据生成与压测工具

## 功能
- 支持生成随机数据（JSON结构）
- 支持TCP/UDP/HTTP/Elasticsearch/Kafka等接口的数据发送
- 支持指定并发数
- 可作为数据写入的压测工具

## 架构
![image](https://user-images.githubusercontent.com/38099986/111066821-f8289900-84fb-11eb-95e3-46d3cd1aef18.png)

stress工具由四个模块组成：
- 数据生成器
  - 使用faker包生成随机的JSON结构数据 
- 可伸缩缓冲池
  - 对channel做了封装，可根据实际需要调整缓冲池的大小
- 数据发送器
  - 分别实现了TCP/UDP/HTTP/Elasticsearch/Kafka客户端接口
- 监控器
  - 监控stress工具运行的指标 

## 用法
```
# 运行
./stress tcp -addr 192.168.10.11 -workers 5

# 查看监控
curl 127.0.0.1:9999/metrics
```
![image](https://user-images.githubusercontent.com/38099986/111067327-bbaa6c80-84fe-11eb-8237-2f598ccd6fb6.png)

![image](https://user-images.githubusercontent.com/38099986/111067561-da5d3300-84ff-11eb-9e81-7e32e820bb17.png)

## 待更新
- 数据中包含@timestamp字段
- 增加TCP连接池
- 支持限速
- 支持更多数据集

