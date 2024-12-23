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

## Project2

### PartA

实验完成当前进度和步骤：

1. 学习Raft一致性共识算法。主要看别人整理好的博客文章https://www.codedump.info/post/20180921-raft/和动画演示理解http://www.kailing.pub/raft/index.html，辅助参考raft官方论文译文https://github.com/maemual/raft-zh_cn/blob/master/raft-zh_cn.md。b站视频学习raft算法https://www.bilibili.com/video/BV1pr4y1b7H5/?spm_id_from=333.337.search-card.all.click。raft学习交互式动画https://raft.github.io/raftscope/index.html。
1. 看功能实现的官方文档和raft文件夹下的实验框架代码。功能实现的官方文档读的不是很明白。
1. 完善`raft/log.go`代码,完善`raft/raft.go`代码.正在编写become等系列函数,这些函数在测试函数中使用,需要去测试代码中看看这些函数怎么用的,怎么测试project2a的功能的.
1. 补完raft.go log.go里的代码，测试2aa 2ab
1. 补完rawnode.go里的代码，测试2ac
1. 完成project2a代码

实验随手记录：

1. raft协议划分为两大部分，分别是领导者选举和日志复制。日志复制部分中，关于节点之间的日志数据不一致部分，不是很理解。

1. Raft 是一种用来**管理复制日志**的算法。日志复制部分也是raft协议中最核心、比较难以理解的部分。安全性部分是对日志复制部分的补充、附加规则，是考虑极端场景下、考虑边界条件下对日志复制过程补充了限制条件。

1. 在采用raft协议的分布式系统中，leader或者follower节点随时可能会崩溃，崩溃期间的节点会丢失许多复制日志的内容。因此raft协议需要有机制保证，当崩溃后的节点恢复后，崩溃节点的复制日志能够恢复到可用状态，日志顺序恢复到和leader节点一致。这个机制就是**一致性检查**。raft协议中日志复制部分，一致性检查是个需要理解的重点。

1. 测试中遇到问题：

   - `log.go`存在许多边界条件需要考虑的情况、下标越界问题：因为实验当中经常碰到要利用日志索引取出日志的情况、日志索引加1减1的情况，因此边界条件没有注意，就会发生日志索引越界问题，不过是小问题，很容易解决，看发生panic的时候，哪里的函数调用出错了，加强一下边界条件的判断即可。

   - 随机的超时时间问题：测试随机超时时间的测试用例过程会很久，网上参考解决方法
   
     > 如果 et 太小，会过早开始选举，导致 term 比测试预期大。如果太大，会很晚发生选举，导致 term 比测试预期小。而且如果按照etcd那样一直递增，最后时间会非常长，直接卡住。我最后把它限制在 10~20 之间，通过测试。
   
   - 测试中，集群中单节点无法选举成为leader问题：follower发生选举时，如果是正常情况下，先becomeCandidate，然后向其他节点sendRequstVote消息，其他节点受到该消息后会决定是否投票，向leader节点发送RequestVoteResponse消息，leader节点在处理这个响应消息中，会判断是否有大多数节点投票。但是，在集群中只有一个单节点的情况下， 测试集不会触发 MsgRequestVoteResponse消息的发送，没有该消息的发送，就不会有处理该消息的过程，就不会有判断是否有大多数节点投过票的过程，就不会有candidate变成leader的结果，最后集群中单节点发起选举后无法变成leader，导致测试无法通过。解决方案就是单节点的情况下，如果发起投票选举，就直接成为leader节点，无需投票，当作特殊情况处理。
   
   - 节点的leader问题。测试集合中，一个节点接受到只可能是leader发来的消息，如果节点原来的leader和发送该消息的leader节点不一致，需要转换leader 节点。

### PartB

实验完成当前进度：

- 看别人实现功能的思路的资料
- 准备去看这部分的实验代码，熟悉一下代码的整体框架和内容。
- 写完了peer_msg_handler.go和peer_storage.go文件需要补充的代码。
- 测试集目前没有通过，有的request timeout 。正在排查原因

实验随手记录：

- project2a部分是实现一个raft算法，2b部分是使用2a部分实现的raft算法

- 服务端接受客户端读写命令到回复客户端的流程大致是：

  > - Clients calls RPC RawGet/RawPut/RawDelete/RawScan
  > - RPC handler calls `RaftStorage` related method
  > - `RaftStorage` sends a Raft command request to raftstore, and waits for the response
  > - `RaftStore` proposes the Raft command request as a Raft log
  > - Raft module appends the log, and persist by `PeerStorage`
  > - Raft module commits the log
  > - Raft worker executes the Raft command when handing Raft ready, and returns the response by callback
  > - `RaftStorage` receive the response from callback and returns to RPC handler
  > - RPC handler does some actions and returns the RPC response to clients.
  
- 测试当中的问题：

  - 我raft/log.go中实现的nextEnts()方法有下标访问出错的问题。重新加强边界条件判断。
  - 测试当中，raftcmd_type会有snap类型的日志条目执行。现在还不是特别理解为什么会有。之前以为2b部分不涉及快照文件，关于快照相关的功能实现都可以先不用考虑，所以碰到snap类型，我就空执行，结果测试的时候，每一个测试集都request timeout，不知道为什么。参考了别人实现的execsnap，也就返回了一个响应，实现之后测试集合就可以通过了。
  - TestOnePartition2B测试集偶尔有失败情况，不知道为什么。知道原因了。这个测试集中，会出现一个网络分区的情况，如果原先的leader节点现在处于少部分节点的分区，那需要重新进行选举，选举出新的leader，该新leader肯定处于大部分节点的分区，这样才不影响读写。而我原先实现raft算法的代码里面，没有考虑这样的情况。


### PartC

- 测试当中的问题：
  - TestRestoreSnapshot2C、TestProvideSnap2C测试不通过。排查过程：发现这些测试都是对raft.go文件的测试，因此先检查了raft.go文件中关于snapshot功能的代码部分。同时，去看了一下TestRestoreSnapshot2C、TestProvideSnap2C的测试代码，发现测试代码当中使用到了配置相关的代码ConfigState，而我关于snapshot功能的部分是没有处理这部分的，因为当时觉得集群更改、配置更改的功能应该到project3才涉及。解决方案：添加上对于snapshot.ConfigState的处理，如果snapshot.ConfigState不为空，就对集群进行设置。
  - TestSnapshotUnreliableRecoverConcurrentPartition2C测试不通过。排查过程：错误堆栈里发现是有一处地方panic了。该panic的地方，发现是entries, err := storage.Entries(firstIndex, lastIndex+1)存在err后，自己手动panic了。对于这个方法而言，如果访问到了压缩之前的日志条目，方法会返回err。为了测试通过，当发现err时，不应手动panic。

## project4

### PartA、B、C

相关资料学习：

- TinyKV事务模型概览https://cn.pingcap.com/blog//tidb-transaction-model/
- Percolator协议https://tikv.org/deep-dive/distributed-transaction/percolator/

实验随手记录：

- 什么是mvcc、percolator、2pc？
- tinykv的transaction是建立在mvcc机制之上的。
- project4是实现分布式事务协议percolator的。project4a实现的mvcc以及project4b、4c都是分布式事务协议percolator的具体代码实现。
- 4b中测试会重复提交，此时lock为空，需要特殊处理
- 事务提交这里有点懵， 不是特别理解怎么进行事务回滚的。答：回滚：删除 meta 中的 Lock 标记 , 删除版本为 startTs 的数据。