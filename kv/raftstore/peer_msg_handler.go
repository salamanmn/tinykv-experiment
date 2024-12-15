package raftstore

import (
	"fmt"
	"reflect"
	"time"

	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
)

type PeerTick int

const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer
	ctx *GlobalContext
}

func newPeerMsgHandler(peer *peer, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer: peer,
		ctx:  ctx,
	}
}

func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}
	// Your Code Here (2B).
	// 1.如果有ready，就就去处理ready
	rawNode := d.RaftGroup
	if !rawNode.HasReady() {
		return
	}
	ready := rawNode.Ready()
	//2.处理ready:持久化日志条目

	applySnapResult, err:= d.peerStorage.SaveReadyState(&ready)
	if err != nil {
		return 
	}

	if applySnapResult != nil {
		if !reflect.DeepEqual(applySnapResult.PrevRegion, applySnapResult.Region) {
			d.peerStorage.SetRegion(applySnapResult.Region)
			d.ctx.storeMeta.Lock()
			d.ctx.storeMeta.regions[applySnapResult.Region.Id] = applySnapResult.Region
			d.ctx.storeMeta.regionRanges.Delete(&regionItem{region: applySnapResult.PrevRegion})
			d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: applySnapResult.Region})
			d.ctx.storeMeta.Unlock()
		}
	}
	
	//3.处理ready:发送给其他节点消息
	if len(ready.Messages) > 0 {
		d.Send(d.ctx.trans, ready.Messages)
	}

	//4.处理ready:已经提交但未应用的日志可以应用了，可以输入状态机，即执行日志中对应的命令
	for _, entry := range ready.CommittedEntries {
		d.peerStorage.applyState.AppliedIndex = entry.Index

		kvWB := new(engine_util.WriteBatch)

		if entry.EntryType == eraftpb.EntryType_EntryConfChange {
			// todo:配置更改的日志应用，即执行
		} else {
			//一般日志的应用，即执行
			d.execEntry(&entry, kvWB)
		}

		// 配置日志相关
		if d.stopped {
			return
		}

		// 写入状态
		kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
		kvWB.WriteToDB(d.peerStorage.Engines.Kv)
	}

	//5.处理ready:ready处理完毕，推进状态机
	d.RaftGroup.Advance(ready)

}

//execEntry：应用日志，即执行日志里保存的命令
func (d *peerMsgHandler) execEntry(entry *eraftpb.Entry, kvWB *engine_util.WriteBatch) {
	raftCmdreq := &raft_cmdpb.RaftCmdRequest{}
	raftCmdreq.Unmarshal(entry.Data)

	// 判断 RegionEpoch
	if raftCmdreq.Header != nil {
		fromEpoch := raftCmdreq.GetHeader().GetRegionEpoch()
		if fromEpoch != nil {
			if util.IsEpochStale(fromEpoch, d.Region().RegionEpoch) {
				resp := ErrResp(&util.ErrEpochNotMatch{})
				d.processProposals(resp,entry,false)
				return
			}
		}
	}

	//todo:administrator requests处理
	if raftCmdreq.AdminRequest != nil {
		switch raftCmdreq.AdminRequest.CmdType {
			case raft_cmdpb.AdminCmdType_CompactLog:
				d.execCompactLog(entry, raftCmdreq.AdminRequest, kvWB)
		}
	}

	// normal requests
	if len(raftCmdreq.Requests) > 0 {
		req := raftCmdreq.Requests[0]
		// fmt.Printf("%x execEntry at term %d, cmdType is %s\n", d.RaftGroup.Raft.GetId(), d.RaftGroup.Raft.Term, req.CmdType)
		switch req.CmdType {
			case raft_cmdpb.CmdType_Get:
				d.execGet(entry, req)
			case raft_cmdpb.CmdType_Put:
				d.execPut(entry, req, kvWB)
			case raft_cmdpb.CmdType_Delete:
				d.execDelete(entry, req, kvWB)
			case raft_cmdpb.CmdType_Snap:
				d.execSnap(entry)
		}
	}
	
}

func (d *peerMsgHandler) execGet(entry *eraftpb.Entry, req *raft_cmdpb.Request) {

	// 应用日志输入到状态机，即执行日志里保存的对应命令
	value, _ := engine_util.GetCF(d.peerStorage.Engines.Kv, req.Get.Cf, req.Get.Key)

	// 命令执行完成后，需要调用回调函数，表明命令已经执行完成，因为命令的执行是异步的，所以需要回调函数来通知上层。
	resps := []*raft_cmdpb.Response{{
		CmdType: raft_cmdpb.CmdType_Get,
		Get: &raft_cmdpb.GetResponse{
			Value: value,
		},
	}}
	cmdResp := &raft_cmdpb.RaftCmdResponse{
		Header:    &raft_cmdpb.RaftResponseHeader{},
		Responses: resps,
	}
	d.processProposals(cmdResp, entry, false)
}

func (d *peerMsgHandler) execPut(entry *eraftpb.Entry, req *raft_cmdpb.Request, kvWB *engine_util.WriteBatch) {
	// 应用日志输入到状态机，即执行日志里保存的对应命令
	kvWB.SetCF(req.Put.Cf, req.Put.Key, req.Put.Value)

	// 命令执行完成后，需要调用回调函数，表明命令已经执行完成，因为命令的执行是异步的，所以需要回调函数来通知上层。
	resps := []*raft_cmdpb.Response{{
		CmdType: raft_cmdpb.CmdType_Put,
		Put:     &raft_cmdpb.PutResponse{},
	}}
	cmdResp := &raft_cmdpb.RaftCmdResponse{
		Header:    &raft_cmdpb.RaftResponseHeader{},
		Responses: resps,
	}
	d.processProposals(cmdResp, entry, false)
}

func (d *peerMsgHandler) execDelete(entry *eraftpb.Entry, req *raft_cmdpb.Request, kvWB *engine_util.WriteBatch) {
	// 应用日志输入到状态机，即执行日志里保存的对应命令
	kvWB.DeleteCF(req.Delete.Cf, req.Delete.Key)

	// 命令执行完成后，需要调用回调函数，表明命令已经执行完成，因为命令的执行是异步的，所以需要回调函数来通知上层。
	resps := []*raft_cmdpb.Response{{
		CmdType: raft_cmdpb.CmdType_Delete,
		Delete:  &raft_cmdpb.DeleteResponse{},
	}}
	cmdResp := &raft_cmdpb.RaftCmdResponse{
		Header:    &raft_cmdpb.RaftResponseHeader{},
		Responses: resps,
	}
	d.processProposals(cmdResp,entry,false)

}

func (d *peerMsgHandler) execSnap(entry *eraftpb.Entry) {

	// 命令执行完成后，需要调用回调函数，表明命令已经执行完成，因为命令的执行是异步的，所以需要回调函数来通知上层。
	resps := []*raft_cmdpb.Response{{
		CmdType: raft_cmdpb.CmdType_Snap,
		Snap: &raft_cmdpb.SnapResponse{
			Region: d.Region(),
		},
	}}
	cmdResp := &raft_cmdpb.RaftCmdResponse{
		Header:    &raft_cmdpb.RaftResponseHeader{},
		Responses: resps,
	}
	d.processProposals(cmdResp,entry,true)
}

func (d *peerMsgHandler) execCompactLog(entry *eraftpb.Entry, req *raft_cmdpb.AdminRequest, kvWB *engine_util.WriteBatch) {
	compactLog := req.GetCompactLog()
	if compactLog.CompactIndex >= d.peerStorage.applyState.TruncatedState.Index {
		d.peerStorage.applyState.TruncatedState.Index= compactLog.CompactIndex
		d.peerStorage.applyState.TruncatedState.Term = compactLog.CompactTerm
		kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
		d.ScheduleCompactLog(compactLog.CompactIndex)//异步完成日志压缩的任务
	}

	// 命令执行完成后，需要调用回调函数，表明命令已经执行完成，因为命令的执行是异步的，所以需要回调函数来通知上层。
	adminResp := &raft_cmdpb.AdminResponse{
		CmdType:    raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogResponse{},
	}
	cmdResp := &raft_cmdpb.RaftCmdResponse{
		Header:        &raft_cmdpb.RaftResponseHeader{},
		AdminResponse: adminResp,
	}
	d.processProposals(cmdResp, entry, false)

}

func (d *peerMsgHandler) processProposals(resp *raft_cmdpb.RaftCmdResponse ,entry *eraftpb.Entry, isExecSnap bool){
	length := len(d.proposals)
	if length > 0 {
		d.dropStaleProposal(entry)
		if len(d.proposals) == 0 {
			return
		}
		p := d.proposals[0]
		if p.index > entry.Index {
			return
		}
		// term 不匹配
		if p.term != entry.Term {
			NotifyStaleReq(entry.Term, p.cb)
			d.proposals = d.proposals[1:]
			return
		}
		// p.index == entry.Index && p.term == entry.Term， 开始执行并回应
		if isExecSnap {
			p.cb.Txn = d.peerStorage.Engines.Kv.NewTransaction(false) 
		}
		p.cb.Done(resp)
		d.proposals = d.proposals[1:]

		return
	}
}

// 丢弃过时的 proposal
func (d *peerMsgHandler) dropStaleProposal(entry *eraftpb.Entry) {
	length := len(d.proposals)
	if length > 0 {
		i := 0
		// 前面未回应的都说明过时了, 直接回应过时错误
		for i < length {
			p := d.proposals[i]
			if p.index < entry.Index {
				p.cb.Done(ErrResp(&util.ErrStaleCommand{}))
				i++
			} else {
				break
			}
		}
		if i == length {
			d.proposals = make([]*proposal, 0)
			return
		}
		d.proposals = d.proposals[i:]
	}
}

func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick:
		d.onTick()
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		log.Infof("%s on split with %v", d.Tag, split.SplitKey)
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		d.startTicker()
	}
}

func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}

	// Check whether the store has the right peer to handle the request.
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	return err
}


func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	// Your Code Here (2B).
	//normal requests处理(注意RaftCmdRequest中的normal request有多个)
	//处理每一个raftcmd请求。即封装成一个proposal消息，然后通过raftNode的propose方法提交到raft中。
	for len(msg.Requests) > 0 {
		request := msg.Requests[0]
		var key []byte
		switch request.CmdType {
		case raft_cmdpb.CmdType_Get:
			key = request.Get.Key
		case raft_cmdpb.CmdType_Put:
			key = request.Put.Key
		case raft_cmdpb.CmdType_Delete:
			key = request.Delete.Key
		}

		//判断key是否在region内
		err = util.CheckKeyInRegion(key, d.Region())
		if err != nil && request.CmdType != raft_cmdpb.CmdType_Snap {
			cb.Done(ErrResp(err))
			msg.Requests = msg.Requests[1:]
			continue
		}

		//封装成proposal消息
		//这里为什么是序列化整个msg，一是因为代码里只有RaftCmdRequest对应的序列化反序列化方法。可以看到我们序列化的msg是，1-n、2-n、3-n这种规律
		//二是因为除了序列化request外，还需要序列化header，因此我们序列化整个msg：raftcmdRequest
		//三是可能是如果仅仅单独序列化request和adminrequest，反序列过程就无法判断成反序列化成哪一个
		//最后反序列化成RaftCmdRequest，我们需要的只有requests中第一个request。
		data, _ := msg.Marshal() 
		p := &proposal{index: d.nextProposalIndex(), term: d.Term(), cb: cb}
		d.proposals = append(d.proposals, p)
		msg.Requests = msg.Requests[1:]
		d.RaftGroup.Propose(data)
	}

	//admin requests处理(注意RaftCmdRequest中的admin request只有一个)
	//admin requests是服务端内部自己产生的，而非客户端发来的，所以应该回调函数为空是正确的
	if msg.AdminRequest != nil {
		request := msg.AdminRequest
		switch request.CmdType {
		case raft_cmdpb.AdminCmdType_CompactLog:
			data, _ := msg.Marshal()
			p := &proposal{index: d.nextProposalIndex(), term: d.Term(), cb: cb}
			d.proposals = append(d.proposals, p)
			d.RaftGroup.Propose(data)
		}
	}
}


func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

func (d *peerMsgHandler) onRaftBaseTick() {
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

func (d *peerMsgHandler) ScheduleCompactLog(truncatedIndex uint64) {
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	log.Debugf("%s handle raft message %s from %d to %d",
		d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	from := msg.GetFromPeer()
	to := msg.GetToPeer()
	log.Debugf("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId())
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

/// Checks if the message is sent to the correct peer.
///
/// Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.
	region := d.Region()
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		log.Infof("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.Infof("%s is stale as received a larger peer %s, destroying", d.Tag, target)
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Infof("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    toPeer,
		ToPeer:      fromPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.Infof("%s receive stale gc msg, ignore", d.Tag)
		return
	}
	log.Infof("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer)
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `snap.SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.Infof("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID)
		return &key, nil
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			log.Infof("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.Infof("%s region overlapped %s %s", d.Tag, existRegion, snapRegion)
		return &key, nil
	}

	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *peerMsgHandler) destroyPeer() {
	log.Infof("%s starts destroy", d.Tag)
	regionID := d.regionId
	// We can't destroy a peer which is applying snapshot.
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	d.ctx.router.close(regionID)
	d.stopped = true
	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		panic(d.Tag + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok {
		panic(d.Tag + " meta corruption detected")
	}
	delete(meta.regions, regionID)
}

func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.RLock()
	defer meta.RUnlock()
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}

	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else {
		return
	}

	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatalf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx)
		panic(err)
	}

	// Create a compact log request and notify directly.
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	d.proposeRaftCommand(request, nil)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}

	if !d.IsLeader() {
		return
	}
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error(err)
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.Infof("%s not leader, skip", d.Tag)
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		log.Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch)
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Infof("%s snap file %s has been compacted, delete", d.Tag, key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					log.Infof("%s snap file %s has been expired, delete", d.Tag, key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.Infof("%s snap file %s has been applied, delete", d.Tag, key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}
