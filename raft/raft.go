// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"math/rand"
	"sort"
	"time"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	//本raft节点需要发送给其他raft节点的消息（发送的动作和时间是由上层应用决定的），我们只需要决定发送的消息类型和内容
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	//记录发出一轮心跳后接受到的心跳状态 ，用于应对网络分区
	heartbeatResp map[uint64]bool
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	raftLog := newLog(c.Storage)
	hardState, confState, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}

	if c.peers == nil {
		c.peers = confState.Nodes
	}

	prs := make(map[uint64]*Progress)
	for _, id := range c.peers {
		prs[id] = &Progress{
			Match: 0,
			Next: 0,
		}
	}

	raft := &Raft{
		id: c.ID,
		Term: hardState.Term,
		Vote: hardState.Vote,
		RaftLog: raftLog,
		Prs: prs,
		State: StateFollower,
		Lead: None,
		votes: make(map[uint64]bool),
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout: c.ElectionTick,
		leadTransferee: 0,
		heartbeatResp: map[uint64]bool{},
	}

	if c.Applied > 0 {
		raft.RaftLog.applied = c.Applied
	}

	return raft
}

//////////////////////////////////////////////////////////////////以下是节点发送消息的具体处理方法（即发送RPC请求）


// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
//将要发送给其他节点的消息放到raft.msgs中，发送的动作和时间由上层应用决定。这个方法是发送追加日志的消息
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	//根据论文，在追加日志过程中，完成日志的一致性，使得其他节点和leader节点的日志强一致
	//确定要发送的消息中的参数
	nextIndex := r.Prs[to].Next

	prevLogIndex := nextIndex - 1
	prevLogTerm, err := r.RaftLog.Term(prevLogIndex)
	//如果nextIndex小于日志的第一个索引，说明to节点的快照落后，则需要发送snapshot
	if err != nil || nextIndex < r.RaftLog.FirstIndex(){
		r.sendSnapshot(to)
		return true
	}
	leaderCommit := r.RaftLog.committed
	
	//从nextIndex开始，获取需要发送的日志，并且设置到msg.Entries中
	firstIndex := r.RaftLog.FirstIndex()
	entries := []*pb.Entry{}
	for i := nextIndex; i <= r.RaftLog.LastIndex(); i++ {
		entries = append(entries, &r.RaftLog.entries[i-firstIndex])
	}

	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To: to,
		From: r.id,
		Term: r.Term,
		Index: prevLogIndex,
		LogTerm: prevLogTerm,
		Entries: entries,
		Commit: leaderCommit,
	}

	r.msgs = append(r.msgs, msg)

	return true
}

func (r *Raft) sendAppendResponse(reject bool, to uint64)  {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From: r.id,
		Term: r.Term,
		Index: r.RaftLog.LastIndex(),
		To: to,
		Reject: reject,
	}
	r.msgs = append(r.msgs,msg)
	return
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
//将要发送给其他节点的消息放到raft.msgs中，发送的动作和时间由上层应用决定。这个方法是发送心跳的消息
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		From: r.id,
		Term: r.Term,
		Commit: r.RaftLog.committed,
		To: to,
	}

	r.msgs = append(r.msgs,msg)
}

// sendHeartbeatResponse sends a heartbeatResponse RPC to the given peer.
//将要发送给其他节点的消息放到raft.msgs中，发送的动作和时间由上层应用决定。这个方法是发送响应心跳的消息
func (r *Raft) sendHeartbeatResponse(to uint64) {
	// Your Code Here (2A).
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From: r.id,
		Term: r.Term,
		To: to,
		Commit: r.RaftLog.committed,
	}

	r.msgs = append(r.msgs, msg)
}

// sendRequestVote sends a requestVote RPC to the given peer.
//将要发送给其他节点的消息放到raft.msgs中，发送的动作和时间由上层应用决定。这个方法是发送请求投票的消息
func (r *Raft) sendRequestVote(to uint64) {
	// Your Code Here (2A).
	lastLogIndex := r.RaftLog.LastIndex()
	lastLogTerm, err := r.RaftLog.Term(lastLogIndex)
	if err != nil {
		panic(err)
	}


	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		To: to,
		From: r.id,
		Term: r.Term,
		Index: lastLogIndex,
		LogTerm: lastLogTerm,
	}

	r.msgs = append(r.msgs, msg)
}

// sendRequestVoteResponse sends a requestVoteResponse RPC to the given peer.
//将要发送给其他节点的消息放到raft.msgs中，发送的动作和时间由上层应用决定。这个方法是发送投票结果的消息
func (r *Raft) sendRequestVoteResponse(reject bool,  to uint64) {
	// Your Code Here (2A).
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From: r.id,
		Term: r.Term,
		To: to,
		Reject: reject,
	}

	r.msgs = append(r.msgs, msg)
}

// sendSnapshot sends a Snapshot RPC to the given peer.

func (r *Raft) sendSnapshot(to uint64){
	var snapshot pb.Snapshot
	var err error
	
	//没有待处理的快照就生成一份
	if !IsEmptySnap(r.RaftLog.pendingSnapshot) {
		snapshot = *r.RaftLog.pendingSnapshot 
	} else {
		snapshot, err = r.RaftLog.storage.Snapshot()
		if err != nil {
			return
		}
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgSnapshot,
		From: r.id,
		Term: r.Term,
		Snapshot: &snapshot,
		To: to,
	}
	r.msgs = append(r.msgs, msg)
	r.Prs[to].Next = snapshot.Metadata.Index + 1
}

//////////////////////////////////////////////////////////////////以下是Tick时间驱动相关方法


// tick advances the internal logical clock by a single tick.
//每调用一次tick，electionElapsed加1,如果是leader节点，则heartbeatElapsed加1。
//超过electionTimeout/heartbeatTimeout，则表示超时，根据不同节点类型，发生相应动作
func (r *Raft) tick() {
	// Your Code Here (2A).
	r.electionElapsed++
	switch r.State {
		case StateFollower:
			if r.electionElapsed >= r.electionTimeout {
				r.electionElapsed = 0
				err := r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
				if err != nil {
					panic(err)
				}
			}
		case StateCandidate:
			if r.electionElapsed >= r.electionTimeout {
				r.electionElapsed = 0
				err := r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
				if err != nil {
					panic(err)
				}
			}
		case StateLeader:
			r.heartbeatElapsed++

			if r.electionElapsed >= r.electionTimeout {
				r.electionElapsed = 0
				heartbeadtRespNum := len(r.heartbeatResp)
				peersNum := len(r.Prs)
				//重置收到的心跳响应状态
				r.heartbeatResp = make(map[uint64]bool)
				r.heartbeatResp[r.id] = true
				// 心跳回应数不超过一半，说明出现了网络分区且leader少数那边，leader 节点需要重新选举
				if heartbeadtRespNum*2 <= peersNum {
					r.handleStartElection()
				}
			}

			if r.heartbeatElapsed >= r.heartbeatTimeout {
				r.heartbeatElapsed = 0
				err := r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat})
				if err != nil {
					panic(err)
				}
			}
	}
	
}

//////////////////////////////////////////////////////////////////以下是节点身份变动相关方法


// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.reset(term)
	r.State = StateFollower
	r.Lead = lead
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.reset(r.Term+1)
	r.State = StateCandidate
	r.Vote = r.id
	r.votes[r.id] = true
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	//1.更新状态信息
	r.reset(r.Term)
	r.State = StateLeader
	r.Lead = r.id

	for followerId := range r.Prs {
		r.Prs[followerId].Next 	= r.RaftLog.LastIndex() + 1
		r.Prs[followerId].Match 	= 0
	}
	//2.追加一条空日志（为了及时提交之前任期内未提交的日志）
	r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{Term: r.Term, Index: r.RaftLog.LastIndex() + 1})
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
	r.Prs[r.id].Match = r.Prs[r.id].Next - 1

	//3.给其他节点同步追加日志
	for followerId := range r.Prs {
		if followerId != r.id {
			r.sendAppend(followerId)
		}
	}

	//4.更新提交索引 todo:是否可以删除
	r.updateCommitIndex()
}

// 重置raft节点的状态信息
func (r *Raft) reset(term uint64)  {
	r.Term = term
	r.Lead = None
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.RandomizedElectionTimeout()
	r.leadTransferee = None
	r.Vote = None
	r.votes = make(map[uint64]bool)
	r.heartbeatResp = make(map[uint64]bool)
	r.heartbeatResp[r.id] = true
}

//随机化选举超时时间
func (r *Raft) RandomizedElectionTimeout() {
	// 限制在 10 ~ 20 之间
	randGenerator := rand.New(rand.NewSource(time.Now().UnixNano()))
	randNumber    := randGenerator.Intn(r.electionTimeout)
	r.electionTimeout += randNumber

	for r.electionTimeout >= 20 {
		r.electionTimeout -= 10
	}
}

//////////////////////////////////////////////////////////////////以下是节点接受消息并处理消息


// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
//节点要接受消息并完成处理，是去调用Step方法
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	var err error = nil
	switch r.State {
	case StateFollower:
		err = r.FollowerStep(m)
	case StateCandidate:
		err = r.CandidateStep(m)
	case StateLeader:
		err = r.LeaderStep(m)
	}
	return err
}


//Follower节点接受到消息的处理方式
func (r *Raft) FollowerStep(m pb.Message) error{
	var err error = nil
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.handleStartElection()
	case pb.MessageType_MsgBeat:
	case pb.MessageType_MsgPropose:
		err = ErrProposalDropped
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
	case pb.MessageType_MsgTransferLeader:
	case pb.MessageType_MsgTimeoutNow:
		r.handleTimeoutNow(m)
	}
	return err
}

//Candidate节点接受到消息的处理方式
func (r *Raft) CandidateStep(m pb.Message) error{
	var err error = nil
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.handleStartElection()
	case pb.MessageType_MsgBeat:
	case pb.MessageType_MsgPropose:
		err = ErrProposalDropped
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResponse(m)
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
	case pb.MessageType_MsgTransferLeader:
	case pb.MessageType_MsgTimeoutNow:
		r.handleTimeoutNow(m)
	}
	return err
}

//Leader节点接受到消息的处理方式
func (r *Raft) LeaderStep(m pb.Message) error{
	var err error = nil
	switch m.MsgType {
	case pb.MessageType_MsgHup:
	case pb.MessageType_MsgBeat:
		r.handleBeat(m)
	case pb.MessageType_MsgPropose:
		if r.leadTransferee == None{
			r.handlePropose(m)
		} else {
			err = ErrProposalDropped
		}
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendResponse(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartbeatResponse(m)
	case pb.MessageType_MsgTransferLeader:
	case pb.MessageType_MsgTimeoutNow:
		r.handleTimeoutNow(m)
	}
	return err
}

//////////////////////////////////////////////////////////////////以下是节点接受到消息后的具体处理方法（即处理RPC请求）

// handleStartElection handle StartElection local request
func (r *Raft) handleStartElection() {
	// Your Code Here (2A)
	//follower节点受到请求选举消息，首先变成candidate节点，然后向其他节点发起投票请求
	//测试集中，如果集群中只有一个单节点，不会触发 MsgRequestVoteResponse，需要特殊考虑
	if len(r.Prs) == 1 {
		r.becomeLeader()
		r.Term++
		return
	}

	r.becomeCandidate()
	for followerId := range r.Prs {
		if followerId != r.id {
			r.sendRequestVote(followerId)
		}
	}
}

// handleTimeoutNow handle TimeoutNow local request
func (r *Raft) handleTimeoutNow(m pb.Message) {
	// Your Code Here (2A)
	r.electionElapsed = 0
	r.handleStartElection()
}

// handlePropose handle propose local request
func (r *Raft) handlePropose(m pb.Message) {
	// Your Code Here (2A)
	//本地追加日志
	lastIndex := r.RaftLog.LastIndex()
	for i := range m.Entries {
		m.Entries[i].Term = r.Term
		m.Entries[i].Index = lastIndex + 1 + uint64(i)
		r.RaftLog.entries = append(r.RaftLog.entries,*m.Entries[i])
	}
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.Prs[r.id].Match + 1

	//给其他节点同步日志
	for followerId := range r.Prs {
		if followerId != r.id {
			r.sendAppend(followerId)
		}
	}

	//单节点的情况下直接更新提交索引
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.Prs[r.id].Match
	}
}


// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	//任期的前置判断
	if r.Term <= m.Term {
		r.Term = m.Term
		if r.State != StateFollower {
			r.becomeFollower(r.Term,None)
		}
	}

	//转换leader
	if r.Lead != m.From {
		r.Lead = m.From
	}

	//如果 Msg 的 Term 小于自己的 Term，则拒绝,否则更新然后继续往下判断；
	if m.Term < r.Term {
		r.sendAppendResponse(true, m.From)
		return
	}

	//如果 prevLogIndex > r.RaftLog.LastIndex()，则拒绝
	if m.Index > r.RaftLog.LastIndex() {
		r.sendAppendResponse(true, m.From)
		return
	}
	//如果接收者日志中没有包含这样一个条目：即该条目的任期在 prevLogIndex 上能和 prevLogTerm 匹配上,则拒绝
	prevLogIndex := m.Index
	prevLogTerm := m.LogTerm
	logTerm, err := r.RaftLog.Term(prevLogIndex)
	if err != nil {
		r.sendAppendResponse(true, m.From)
		return
	}
	if logTerm !=  prevLogTerm{
		r.sendAppendResponse(true, m.From)
		return
	}

	//追加新的日志条目，如果存在冲突的未提交的日志，则需要全部删除，保证和leader日志强一致（注意不冲突情况下，应该不删除后面的日志）
	for i := range m.Entries {  //追加新日志
		appendLogindex := m.Entries[i].Index
		oldlogTerm, _ := r.RaftLog.Term(appendLogindex)
		appendLogTerm := m.Entries[i].Term
		if appendLogindex > r.RaftLog.LastIndex() {
			r.RaftLog.entries = append(r.RaftLog.entries, *m.Entries[i])
		} else if (oldlogTerm != appendLogTerm) {
			r.RaftLog.entries = r.RaftLog.entries[:appendLogindex - r.RaftLog.FirstIndex()]
			r.RaftLog.stabled = min(r.RaftLog.stabled, appendLogindex-1)
			r.RaftLog.entries = append(r.RaftLog.entries, *m.Entries[i])
		}
	}

	//更新节点最后追加的日志索引
	r.RaftLog.lastAppend = m.Index + uint64(len(m.Entries))

	//更新自己的term,发送响应消息
	r.sendAppendResponse(false, m.From)

	//更新commitIndex
	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Commit, r.RaftLog.lastAppend)
	}
}


// handleAppendResponse handle AppendEntriesResponse RPC request
func (r *Raft) handleAppendResponse(m pb.Message) {
	//其他节点追加日志失败
	if m.Reject {
		r.Prs[m.From].Next = min(m.Index+1, r.Prs[m.From].Next - 1)
		r.sendAppend(m.From)
		return
	}

	//其他节点追加日志成功
	r.Prs[m.From].Next = m.Index + 1
	r.Prs[m.From].Match = m.Index
	
	//更新commitIndex，如果大多数节点的日志都成功追加
	//如何判断大多数节点的日志都追加成功，然后更新commitIndex呢？如果存在大多数节点的日志索引matchIndex>commitIndex且是当前任期内产生的日志，此时就可以更新commitIndex。
	oldCommitIndex := r.RaftLog.committed
	r.updateCommitIndex()
	// 更新完后向所有节点再发一个Append，用于同步committed
	if r.RaftLog.committed != oldCommitIndex{
		for pr := range r.Prs {
			if pr != r.id {
				r.sendAppend(pr)
			}
		}
	}
}

//更新leader节点的日志索引
func (r *Raft) updateCommitIndex() {
	//记录所有的节点的matchIndex
	matchIndexs := []uint64{}
	for _, pr := range r.Prs {
		matchIndexs = append(matchIndexs, pr.Match)
	}

	//排序matchIndex，取出中间位置的索引，即为大多数节点的日志索引
	sort.Slice(matchIndexs, func(i, j int) bool {
		return matchIndexs[i] < matchIndexs[j]
	})

	quorumIndex := matchIndexs[(len(matchIndexs)-1)/2]

	var i uint64
	for i = quorumIndex; i > r.RaftLog.committed; i-- {
		if term, err := r.RaftLog.Term(i); err == nil && term == r.Term {
			break
		}
	}

	r.RaftLog.committed = i
	
}

// handleRequestVote handle RequestVote RPC request
func (r *Raft) handleRequestVote(m pb.Message) {
	//任期的前置判断
	if r.Term < m.Term {
		r.Term = m.Term	
		r.Vote = None
		if r.State != StateFollower {
			r.becomeFollower(m.Term, None)
		}
	}
	//如果 Msg 的 Term 小于自己的 Term，则拒绝,否则更新然后继续往下判断；
	if m.Term < r.Term {
		r.sendRequestVoteResponse(true, m.From)
		return
	}
	
	//如果节点已经投过票，则拒绝
	if r.Vote != None && r.Vote != m.From {
		r.sendRequestVoteResponse(true, m.From)
		return
	}

	//如果节点的日志更“新"，则拒绝
	lastLogIndex :=  r.RaftLog.LastIndex() 
	lastLogTerm, err := r.RaftLog.Term(lastLogIndex)
	if err != nil {
		panic(err)
	}

	if (lastLogTerm > m.LogTerm) || (lastLogTerm == m.LogTerm && lastLogIndex > m.Index) {
		r.sendRequestVoteResponse(true, m.From)
		return
	}

	//同意投票给请求投票的节点
	r.Vote = m.From
	r.sendRequestVoteResponse(false, m.From)
}

// handleRequestVoteResponse handle RequestVoteResponse RPC request
func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	// Your Code Here (2A).
	//更新投票给candidate节点的结果
	r.votes[m.From] = !m.Reject
	quorum := len(r.Prs)/2 + 1 //集群大多数节点的数量
	agreeNum, rejectNum := 0, 0
	for _, vote := range r.votes {
		if vote {
			agreeNum++
		} else {
			rejectNum++
		}
	}
	if agreeNum >= quorum {
		r.becomeLeader()
	} else if rejectNum >= quorum {
		r.becomeFollower(r.Term, None)
	}
}


// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	//任期的前置判断
	if r.Term <= m.Term {
		r.Term = m.Term
		if r.State != StateFollower {
			r.becomeFollower(m.Term, None)
		}
	}

	//转换leader
	if r.Lead != m.From {
		r.Lead = m.From
	}
	//重置投票时间
	r.electionElapsed = 0
	//响应
	r.sendHeartbeatResponse(m.From)
}

// handleHeartbeatResponse handle HeartbeatResponse RPC request
func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	// Your Code Here (2A).

	//任期的前置判断
	if r.Term < m.Term {
		r.Term = m.Term
		if r.State != StateFollower {
			r.becomeFollower(r.Term,None)
		}
	}
	//收到心跳
	r.heartbeatResp[m.From] = true

	//通过 m.Commit 判断节点是否落后了，如果是，则进行日志追加；
	if m.Commit < r.RaftLog.committed {
		r.sendAppend(m.From)
	}
}

// handleBeat handle beat local request
func (r *Raft) handleBeat(m pb.Message) {
	// Your Code Here (2A).
	for followerId := range r.Prs {
		if followerId != r.id {
			r.sendHeartbeat(followerId)
		}
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	//任期的前置判断
	if r.Term < m.Term {
		r.Term = m.Term
		if r.State != StateFollower {
			r.becomeFollower(r.Term,None)
		}
	}

	if m.Term < r.Term {
		return
	}

	if r.Lead != m.From {
		r.Lead = m .From
	}

	snapShotIndex := m.Snapshot.Metadata.Index
	snapShotTerm := m.Snapshot.Metadata.Term
	snapShotConfState := m.Snapshot.Metadata.ConfState

	if snapShotIndex < r.RaftLog.committed || snapShotIndex < r.RaftLog.FirstIndex(){
		return
	}

	// 将快照文件包含的日志全部删除
	if len(r.RaftLog.entries) > 0 {
		if snapShotIndex >= r.RaftLog.LastIndex() {
			r.RaftLog.entries = nil
		} else {
			r.RaftLog.entries = r.RaftLog.entries[snapShotIndex - r.RaftLog.FirstIndex() + 1 :]
		}
	}

	r.RaftLog.committed = snapShotIndex
	r.RaftLog.applied = snapShotIndex
	r.RaftLog.stabled = snapShotIndex

	// 集群节点设置（变更）
	if snapShotConfState != nil {
		r.Prs = make(map[uint64]*Progress)
		for _, node := range snapShotConfState.Nodes {
			r.Prs[node] = &Progress{}
			r.Prs[node].Next = r.RaftLog.LastIndex() + 1
			r.Prs[node].Match = 0
		}
	}

	// 加一个空条目，使得通过LastIndex()获取的lastIndex、lastTerm正确
	//该空条目仅仅用来表示快照文件的最后一个日志条目的索引和任期
	if r.RaftLog.LastIndex() < snapShotIndex {
		entry := pb.Entry{
			EntryType: pb.EntryType_EntryNormal,
			Index: snapShotIndex,
			Term: snapShotTerm,
		}
		r.RaftLog.entries = append(r.RaftLog.entries, entry)
	}

	r.RaftLog.pendingSnapshot = m.Snapshot
	r.sendAppendResponse(false,m.From)

}

//////////////////////////////////////////////////////////////////节点的软状态、硬状态相关
func (r *Raft) softState() *SoftState { 
	return &SoftState{Lead: r.Lead, RaftState: r.State} 
}

func (r *Raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}

func (r *Raft) GetId() uint64 {
	return r.id
}

//////////////////////////////////////////////////////////////////


// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
