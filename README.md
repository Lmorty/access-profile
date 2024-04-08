# 项目介绍

本项目是为人群画像系统一期开发的,主要实现数据的取值压缩与同步。

后期会将sql代码也封装进来形成独立的画像项目。



## 模块介绍

#### common

**公共模块**

存放了通用的工具类和一期其他自定义类(HiveTable描述类和Hive字段描述类)



#### dao

**数据访问模块**

目前只开发了Hive的 (实现方案为HiveParser + FreeMarker)

```she
MainEntry : GenerateHiveEntityUtil 
可以将DDL转换为对应目录下的实体类,包含字段信息和表信息
```

后面可以继续拓展Mysql和ClickHouse



#### metadata

**元数据更新模块**

为了解决人群系统元数据同步任务,现在实现的方式很简单,往后要与人群系统一期改良这部分



#### core

**核心模块**

```shell
MainEntry :AccessUserProfileApp 任务的入口
其余看类注释即可
```

<!--主要是Bitmap的函数与sql的生成以及,groupingID的解析与序列化后导出-->



