package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	//给req中的keys上latch锁，避免并发问题。记得释放latch锁
	keysToLatch := make([][]byte, 0)
	keysToLatch = append(keysToLatch, req.Key)
	server.Latches.WaitForLatches(keysToLatch)
	defer server.Latches.ReleaseLatches(keysToLatch)

	//执行获取key的value值操作。获取前需要判断该key是否已经被其他事务锁住，这里是通过获取key的lock结构体判断，而非latch
	resp := new(kvrpcpb.GetResponse)
	
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	defer reader.Close()

	mvccTxn := mvcc.NewMvccTxn(reader, req.Version)

	//检查key的lock，看key是否已经被其他事务锁住
	lock, err := mvccTxn.GetLock(req.Key)
	if err != nil {
		return resp, err
	}

	if lock != nil && lock.IsLockedFor(req.Key, req.Version, resp) {
		return resp, nil
	}

	//key没有被其他事务锁住，获取key对应的value值
	value, err := mvccTxn.GetValue(req.Key)
	if err != nil {
		return resp, err
	}
	if value == nil {
		resp.NotFound = true
	}
	resp.Value = value
	return resp, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	//给req中的keys上latch锁，避免并发问题。记得释放latch锁
	keysToLatch := make([][]byte, 0)
	for _, mutaion := range req.Mutations {
		keysToLatch = append(keysToLatch, mutaion.Key)
	}
	server.Latches.WaitForLatches(keysToLatch)
	defer server.Latches.ReleaseLatches(keysToLatch)

	//执行预写操作。执行前需要判断1.预写的这些 key 的最新 Write，如果存在，且其 commitTs 大于当前事务的 startTs，说明存在 write conflict
	//2.预写的这些key是否已经被其他事务锁住，这里是通过获取key的lock结构体判断，而非latch。
	resp := new(kvrpcpb.PrewriteResponse)

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	defer reader.Close()
	
	mvccTxn := mvcc.NewMvccTxn(reader, req.StartVersion)

	//检查1
	for _, mutation := range req.Mutations {
		write, commitTs, err := mvccTxn.MostRecentWrite(mutation.Key)
		if err != nil {
			return resp, err
		}

		if write != nil && commitTs > req.StartVersion {
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					StartTs:  req.StartVersion,
					ConflictTs: commitTs,
					Key:      mutation.Key,
					Primary:   req.PrimaryLock,
				},
			})
			return resp, nil
		}

	}

	//检查2
	for _, mutation := range req.Mutations {
		lock, err := mvccTxn.GetLock(mutation.Key)
		if err != nil {
			return resp, err
		}

		if lock != nil && lock.Ts <= req.StartVersion {
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{
				Locked: lock.Info(mutation.Key),
			})
			return resp, nil
		}
	}

	//检查通过，执行预写(预写==写入default和写入lock)
	for _, mutation := range req.Mutations {
		var writeKind mvcc.WriteKind
		switch mutation.Op {
		case kvrpcpb.Op_Put:
			mvccTxn.PutValue(mutation.Key, mutation.Value)
			writeKind = mvcc.WriteKindPut
		case kvrpcpb.Op_Del:
			mvccTxn.DeleteValue(mutation.Key)
			writeKind = mvcc.WriteKindDelete
		case kvrpcpb.Op_Rollback:
			writeKind = mvcc.WriteKindRollback
		}

		mvccTxn.PutLock(mutation.Key, &mvcc.Lock{
			Kind: writeKind,
			Ts: mvccTxn.StartTS,
			Primary: req.PrimaryLock,
			Ttl: req.LockTtl,
		})
	}

	err = server.storage.Write(req.Context, mvccTxn.Writes())
	if err != nil {
		return resp, err
	}
	return resp, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	//给req中的keys上latch锁，避免并发问题。记得释放latch锁
	keysToLatch := make([][]byte, 0)
	keysToLatch = append(keysToLatch, req.Keys...)
	server.Latches.WaitForLatches(keysToLatch)
	defer server.Latches.ReleaseLatches(keysToLatch)

	//执行提交操作。执行前需要检查 Lock.StartTs 和当前事务的 startTs 是否一致
	resp := new(kvrpcpb.CommitResponse)

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	defer reader.Close()
	
	mvccTxn := mvcc.NewMvccTxn(reader, req.StartVersion)

	for _, key := range req.Keys {
		lock, err := mvccTxn.GetLock(key)
		if err != nil {
			return resp, err
		}
		//重复提交和回滚提交时lock为空
		if lock == nil {
			write, _, err := mvccTxn.CurrentWrite(key)
			if err != nil {
				return resp, err
			}

			if write != nil && write.StartTS == mvccTxn.StartTS && write.Kind == mvcc.WriteKindRollback {
				resp.Error = &kvrpcpb.KeyError{
					Retryable: "true",
				}
				return resp, nil
			}
		} else if lock.Ts != mvccTxn.StartTS {
			resp.Error = &kvrpcpb.KeyError{
				Retryable: "true",
			}
			return resp, nil
		}

	}

	//检查通过，执行提交(写入write和移除lock)
	for _, key := range req.Keys {
		lock, err := mvccTxn.GetLock(key)
		if err != nil {
			return resp, err
		}
		if lock == nil { //重复提交时lock为空
			continue
		}
		mvccTxn.PutWrite(key, req.CommitVersion, &mvcc.Write{
			Kind: lock.Kind,
			StartTS: mvccTxn.StartTS,
		})

		mvccTxn.DeleteLock(key)
	}

	err = server.storage.Write(req.Context, mvccTxn.Writes())
	if err != nil {
		return resp, err
	}
	return resp, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	resp := new(kvrpcpb.ScanResponse)

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	defer reader.Close()
	mvccTxn := mvcc.NewMvccTxn(reader, req.Version)
	scanner := mvcc.NewScanner(req.StartKey, mvccTxn)
	defer scanner.Close()

	for i := uint32(0); i < req.Limit; i++ {
		if !scanner.Iterator.Valid() {
			break
		}
		key, val, err := scanner.Next()
		if err != nil {
			return resp, err
		}
		if key == nil {
			continue
		}

		//检查锁状态
		lock, err := mvccTxn.GetLock(key)
		if err != nil {
			return resp, err
		}
		if lock != nil && lock.Ts < mvccTxn.StartTS {
			resp.Pairs = append(resp.Pairs, &kvrpcpb.KvPair{
				Error: &kvrpcpb.KeyError{
					Locked: lock.Info(key),
				}})
			continue
		}

		//检查通过，设置值
		if val != nil {
			resp.Pairs = append(resp.Pairs, &kvrpcpb.KvPair{
				Key: key,
				Value: val,
			})
		}

		
	}
	return resp, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	resp := new(kvrpcpb.CheckTxnStatusResponse)
	
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	defer reader.Close()
	mvccTxn := mvcc.NewMvccTxn(reader, req.LockTs)
	//通过 CurrentWrite() 获取 primary key 的 Write
	write, commitTs, err := mvccTxn.CurrentWrite(req.PrimaryKey)
	if err != nil {
		return resp, err
	}
	//通过 GetLock() 获取 primary key 的 Lock
	lock, err := mvccTxn.GetLock(req.PrimaryKey)
	if err != nil {
		return resp, err
	}
	//事务处于提交状态
	//如果 primary key 的 Write 不为空且 Kind 不是 Rollback，则说明 primary key 已经提交，返回 CommitTs
	if write != nil && write.Kind != mvcc.WriteKindRollback {
		resp.CommitVersion = commitTs
		return resp, nil
	}

	//事务处于回滚状态
	if lock == nil {
		if write != nil && write.Kind == mvcc.WriteKindRollback {
			resp.CommitVersion = 0
			resp.LockTtl = 0
			resp.Action = kvrpcpb.Action_NoAction
			return resp, nil
		} else { //pass TestCheckTxnStatusNoLockNoWrite4C
			mvccTxn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
				Kind: mvcc.WriteKindRollback,
				StartTS: req.LockTs,
			})
			err := server.storage.Write(req.Context, mvccTxn.Writes())
			if err != nil {
				return resp, err
			}
			resp.Action = kvrpcpb.Action_LockNotExistRollback
			return resp, nil
		}
	}

	//事务处于被锁定状态下，有锁超时和锁不超时两种情况
	reqCurTs := mvcc.PhysicalTime(req.CurrentTs)
	lockTs := mvcc.PhysicalTime(lock.Ts)
	//锁超时，需要删除锁和删除数据值，打上回滚标记,完成事务回滚
	if reqCurTs > lockTs && reqCurTs - lockTs >= lock.Ttl {
		mvccTxn.DeleteLock(req.PrimaryKey)
		mvccTxn.DeleteValue(req.PrimaryKey)
		mvccTxn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
			Kind: mvcc.WriteKindRollback,
			StartTS: req.LockTs,
		})
		err = server.storage.Write(req.Context, mvccTxn.Writes())
		if err != nil {
			return resp, err
		}
		resp.Action = kvrpcpb.Action_TTLExpireRollback
	} else { //锁不超时，直接返回，无需任何操作
		resp.Action = kvrpcpb.Action_NoAction
	}
	return resp, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	resp := new(kvrpcpb.BatchRollbackResponse)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	defer reader.Close()

	mvccTxn := mvcc.NewMvccTxn(reader, req.StartVersion)
	//检查事务是否已经提交
	for _, key := range req.Keys {
		write, _, err := mvccTxn.CurrentWrite(key)
		if err != nil {
			return resp, err
		}
		if write != nil && write.Kind != mvcc.WriteKindRollback {
			resp.Error = &kvrpcpb.KeyError{
				Abort: "true",
			}
			return resp, nil
		}
	}

	//执行回滚操作
	for _, key :=range req.Keys {
		write, _, err := mvccTxn.CurrentWrite(key)
		if err != nil {
			return resp, err
		}
		lock, err := mvccTxn.GetLock(key)
		if err != nil {
			return resp, err
		}

		//已经完成回滚
		if write != nil && write.Kind == mvcc.WriteKindRollback {
			continue
		}

		//即使key被其他事务lock了，也要进行回滚标记
		if lock != nil && lock.Ts != mvccTxn.StartTS {
			mvccTxn.PutWrite(key, mvccTxn.StartTS, &mvcc.Write{
				Kind: mvcc.WriteKindRollback,
				StartTS: mvccTxn.StartTS,
			})
			continue
		}
		//回滚操作（删除锁和删除数据值，打上回滚标记）
		mvccTxn.DeleteLock(key)
		mvccTxn.DeleteValue(key)
		mvccTxn.PutWrite(key, mvccTxn.StartTS, &mvcc.Write{
			StartTS: mvccTxn.StartTS,
			Kind:    mvcc.WriteKindRollback,
		})

	}
	
	err = server.storage.Write(req.Context, mvccTxn.Writes())
	if err != nil {
		return resp, err
	}

	return resp, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	resp := new(kvrpcpb.ResolveLockResponse)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	defer reader.Close()

	mvccTxn := mvcc.NewMvccTxn(reader, req.StartVersion)
	//获取到含有 Lock 的所有 key
	iterator := reader.IterCF(engine_util.CfLock)
	defer iterator.Close()
	var keys [][]byte
	for ; iterator.Valid(); iterator.Next() {
		item := iterator.Item()
		key := item.KeyCopy(nil)
		value, err := item.ValueCopy(nil)
		if err != nil {
			return resp, err
		}
		lock, err := mvcc.ParseLock(value)
		if err != nil {
			return resp, err
		}
		if lock.Ts == mvccTxn.StartTS {
			keys = append(keys, key)
		}
	}
	// 如果 req.CommitVersion == 0，则调用 KvBatchRollback() 将这些 key 全部回滚
	if req.CommitVersion == 0 {
		batchRollBackreq := &kvrpcpb.BatchRollbackRequest{
			Context:           req.Context,
			StartVersion:      mvccTxn.StartTS,
			Keys:              keys,
		}
		batchRollBackresp, err := server.KvBatchRollback(context.Background(), batchRollBackreq)
		if err != nil {
			return resp, err
		}
		resp.Error = batchRollBackresp.GetError()
		resp.RegionError = batchRollBackresp.GetRegionError()
	} else {
		commitReq := &kvrpcpb.CommitRequest{
			Context:       req.Context,
			Keys:          keys,
			CommitVersion: req.CommitVersion,
			StartVersion: mvccTxn.StartTS,
		}

		commitResp, err := server.KvCommit(context.Background(), commitReq)
		if err != nil {
			return resp, err
		}
		resp.Error = commitResp.GetError()
		resp.RegionError = commitResp.GetRegionError()
	}
	return resp, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
