---
title: 一次线上CPU突然飙升的排查经历
tags:
  - 日常运维
originContent: ''
categories:
  - Technology
toc: true
date: 2020-04-09 16:21:09
---

# 事故背景
某日，运维突然发来一个截图，紧接着在群里把我直接@出来
![企业微信截图_f89b75554ec24ec9a2809432b01cdf31.png](/images/2020/04/09/e79401f0-7a3a-11ea-be0b-d9537b42a236.png)
一看图，发现我的应用占用的CPU高达2317.2%，吓得尿了一库，火急火燎地赶紧开始排查问题。

# 排查过程
## 1.观察现象
> 线上出现了问题，当然是去看我们应用里面打出的日志。

这时候，我登上了服务器，看到日志大小从上个星期开始逐渐飙升，文件大小从原来的几十MB上升到几GB，再到今天的十七GB。然后从部分的日志里面看，一直有人在使用我们的业务，并且存在疯狂刷请求的现象。

于是，得出初步的怀疑：QPS很高，或者存在自动死循环调用方法的嫌疑，导致后台疯狂运转。

## 2.初步排查
> QPS可以通过nginx的请求日志来判断

这时候，我联系了运维小哥，问了一下我们应用今天的访问情况，结果运维小哥说：到目前为止，只有5000+访问日志。也就是再怎么大也是5000+的请求量，不存在有人恶意刷我们后台的情况。

这时候，有点懵圈了。那为啥回存在疯狂调用实时查询的那个方法？
```java
/**
 * 实时查询-数据实时查询
 * 支持多个设备同时查询
 */
 public List getRealtimeData(String clientId, String env, String deviceId, String app, String platform,
                                String vercode, String batch, String eventId, String userId) {
        String redisKafkaCache = getRedisKafkaCache(env, app);
           ...
	   ...

        //批量从redis获取从kafka消费过来的数据
        List<String> deviceList = getDeviceList(env, deviceId);
        log.info("[获取设备列表]:env={},deviceId={},deviceList size={},deviceList={}", env, deviceId, deviceList.size(), Joiner.on(",").join(deviceList));

        Iterator<String> iterator = deviceList.iterator();
        List<JSONObject> messages = new ArrayList<>();
        while (iterator.hasNext()) {
            deviceId = iterator.next();
            ...
	    ...
	    //获取数据
	}
}
```
又仔细观察了一下日志，统计了一下日志里面`获取设备列表`这个关键字也确实不多，但是在观察到有些请求是查询测试环境的数据时，发现测试环境的设备列表长度高达10000+，也就是有10000+设备，然后每个设备都去请求两次redis获取数据。这时候的我，似乎发现了什么猫腻。

## 3.确认问题
> CPU占用过高一般是因为业务代码中有CPU密集型任务、死循环或者GC太频繁等情况

上面说到发现10000+设备，每个设备都去请求两次redis，那就是20000+请求，但是为啥会导致CPU过高？
我决定了参考网上大神们相似的CPU过高的排查过程：

1. 用top命令查看我们pid
2. 查询该进程内最耗费CPU的线程：top -Hp pid
3. 转换线程id为16进制：printf "%x\n" tid（就是top -Hp列表展示的pid）
4. 通过jstack查找特定线程的信息
5. 额外：使用jstat -gcutil pid interval查看gc情况

经过一连串的操作，GC是正常的，确实是三秒一次、每次10000+接近死循环的操作导致CPU过高。

## 4.解决问题
最终问题还是回归到需求，当时的需求是查询测试环境的实时数据时，可以选择所有曾经进入到测试kafka topic的设备，但是有效的设备并不多，这也造成一些废弃的设备用于无效的查询（业务相关）。

解决方式：

1. 对于测试环境的设备，只保留两天内活跃的
2. 限制选定N个设备用于查询数据

