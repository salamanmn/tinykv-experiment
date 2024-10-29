#  TinyKV-系统能力培养实验过程记录

[TOC]

## Project1

### 相关资料参考

1. 了解tinykv设计的核心理念：[三篇文章了解 TiDB 技术内幕 - 说存储 | PingCAP 平凯星辰](https://cn.pingcap.com/blog/tidb-internal-1/)博客
2. 了解实验通关的思路：个人博客、官方文档
   - https://tanxinyu.work/tinykv/博客
   - https://github.com/Smith-Cruise/TinyKV-White-Paper博客
   - https://github.com/talent-plan/tinykv?tab=readme-ov-file Tinykv官方github仓库提供的文档
3. `Project1`中涉及知识的学习：
   - `badger`DB的使用：https://github.com/Connor1996/badger

### 实现思路

#### Implement standalone storage engine

需要修改的代码文件`standalone_storage.go`。storage存储需要依靠badger数据库，badger数据库对外提供的api都已经在`engine_util`包进行了二次的封装，实现了不带CF和带CF的增删改查。我们只需要按照`storage`接口规定的方法，使用`engine_util`包就能完成规定的功能。主要是编写`Reader`方法和`Write`方法。

1. **`Reader`实现**

   `Reader`方法需要返回接口变量`storage.StorageReader`，所以需要自定义一个实现`storage.StorageReader`接口的结构体，按接口规范实现该结构体上的方法。具体的功能实现直接使用`engine_util`包中的方法

2. **`Write`实现**

   `Write`方法接受的参数有 `[]storage.Modify`类型。该切片类型代表着一批量的写操作（包括更改和删除），而代码框架里提供了批量写操作的工具类（位于`write_batch.go`文件中），直接使用即可。其实应该也可以不用批量写操作的工具类（位于`write_batch.go`），可以遍历切片，判别一下每一个`modify`里的接口字段Data的类型是`Put`还是`Delete`，然后用`badger`的原生api完成。

#### Implement service handlers

需要修改的代码文件`raw_api.go`。需要实现RawGet、RawPut、RawDelete、RawScan四个service handler，这些方法的定义都是接受请求参数req和上下文context，返回响应参数resp和错误类型error。因此这些方法的实现，基本上应该都是使用req里的某些参数调用上一步storage实现好的方法，完成功能后封装resp返回。

1. `RawGet`实现：调用`storage.Reader`，调用`Reader.GetCF`,得到`value`，封装响应`resp`。注意：如果用不存在的key去查询，会返回ErrNotFound的错误，根据文档要求，查询不到的情况下，返回的错误应该是nil，所以需要再做一次处理。
2. `RawPut`实现：调用`storage.Write`。调用前，封装好`[]storage.Modify`参数进行传递。
3. `RawDelete`实现：同`RawPut`
4. `RawScan`实现：调用`storage.Reader`，调用`Reader.IterCF`,得到`iterator`，迭代遍历，封装响应`resp`