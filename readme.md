## 需求

项目中mysql有一些表数据增长的很快，但是呢这个数据最多只留最近的一个月就行，一个月以前的记录不太重要了，但是又不能删除。为了保证这个表的查询速度，需要一个简单的备份表，把数据倒进去。

## 实现方法
写了一个小脚本，用来做定时任务，把这个表某段时间的数据备份到备份表中，核心就是个简单的sql。
详细请参考代码。

## 部署

1. 确保服务器具备python3环境
2. 拷贝process.py到任意目录，然后增加可执行权限(chmod)
3. 编辑linux定时任务，按照自己想要的执行周期调用即可

## tips

跨年的时候，需要带参数执行最后一个月的备份，示例:

```
python3 process.py 201812
```