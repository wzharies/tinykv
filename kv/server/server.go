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

	// (Used in 4A/4B)
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
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			return &kvrpcpb.GetResponse{RegionError: regionErr.RequestErr}, err
		}
		return nil, err
	}
	defer reader.Close()

	lock, err := mvcc.NewMvccTxn(reader, req.Version).GetLock(req.Key)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			return &kvrpcpb.GetResponse{RegionError: regionErr.RequestErr}, err
		}
		return nil, err
	}
	if lock != nil && lock.Ts <= req.Version {
		return &kvrpcpb.GetResponse{
			Error: &kvrpcpb.KeyError{
				Locked: &kvrpcpb.LockInfo{
					PrimaryLock: lock.Primary,
					LockVersion: lock.Ts,
					Key:         req.Key,
					LockTtl:     lock.Ts,
				},
			},
		}, nil
	}
	value, err := mvcc.NewMvccTxn(reader, req.Version).GetValue(req.Key)

	if value == nil {
		return &kvrpcpb.GetResponse{
			NotFound: true,
		}, nil
	}

	return &kvrpcpb.GetResponse{
		Value: value,
	}, nil

}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	// TODO
	// 1. 创建MVCC事务 MVCC TXN
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			return &kvrpcpb.PrewriteResponse{RegionError: regionErr.RequestErr}, err
		}
		return nil, err
	}
	defer reader.Close()

	mvccTxn := mvcc.NewMvccTxn(reader, req.StartVersion)
	prewriteResponse := kvrpcpb.PrewriteResponse{}
	// 2.1 遍历所有的mutations
	for i := range req.Mutations {
		mutation := req.Mutations[i]
		recentWrite, rt, err := mvccTxn.MostRecentWrite(mutation.Key)
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				return &kvrpcpb.PrewriteResponse{RegionError: regionErr.RequestErr}, err
			}
			return nil, err
		}
		// 2.2 判断是否和最近的相同key的时候是否有时间的冲突
		if recentWrite != nil && rt >= req.StartVersion {
			prewriteResponse.Errors = append(prewriteResponse.Errors, &kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					StartTs:    req.StartVersion,
					ConflictTs: rt,
					Key:        mutation.Key,
					Primary:    req.PrimaryLock,
				},
			})
			continue
		}
		// 2.3 获取lock 判断锁是否和当前有冲突

		lock, err := mvccTxn.GetLock(mutation.Key)
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				return &kvrpcpb.PrewriteResponse{RegionError: regionErr.RequestErr}, err
			}
			return nil, err
		}
		if lock != nil && lock.Ts < req.StartVersion {
			prewriteResponse.Errors = append(prewriteResponse.Errors, &kvrpcpb.KeyError{
				Locked: &kvrpcpb.LockInfo{
					PrimaryLock: req.PrimaryLock,
					LockVersion: lock.Ts,
					Key:         mutation.Key,
					LockTtl:     lock.Ttl,
				},
			})
			continue
		}
		// 2.4 根据炒作类型 判断是要进行什么操作 插入值和加锁

		switch mutation.Op {
		case kvrpcpb.Op_Put:
			mvccTxn.PutValue(mutation.Key, mutation.Value)
			mvccTxn.PutLock(mutation.Key, &mvcc.Lock{
				Primary: req.PrimaryLock,
				Ts:      req.StartVersion,
				Ttl:     req.LockTtl,
				Kind:    mvcc.WriteKindPut,
			})
		case kvrpcpb.Op_Del:
			mvccTxn.DeleteValue(mutation.Key)
			mvccTxn.PutLock(mutation.Key, &mvcc.Lock{
				Primary: req.PrimaryLock,
				Ts:      req.StartVersion,
				Ttl:     req.LockTtl,
				Kind:    mvcc.WriteKindDelete,
			})
		}
	}

	// 3. 判断是否有错误 有错误 返回错误
	if prewriteResponse.Errors != nil && len(prewriteResponse.Errors) > 0 {
		return &prewriteResponse, nil
	}
	// 4.调用存储引擎写入数据
	err = server.storage.Write(req.Context, mvccTxn.Writes())
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			return &kvrpcpb.PrewriteResponse{RegionError: regionErr.RequestErr}, err
		}
		return nil, err
	}
	return &kvrpcpb.PrewriteResponse{}, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	// // 1. 创建MVCC事务 MVCC TXN
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			return &kvrpcpb.CommitResponse{RegionError: regionErr.RequestErr}, err
		}
		return nil, err
	}
	defer reader.Close()
	mvccTxn := mvcc.NewMvccTxn(reader, req.StartVersion)
	// Latch Wait全局锁
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)
	// 判断所有key 的lock是否 是否和当前的版本一致，如果不一致就需要重试，否则写入数据 并且删除锁
	for i := range req.Keys {
		key := req.Keys[i]
		// 检查重复提交
		write, _, err := mvccTxn.CurrentWrite(key)
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				return &kvrpcpb.CommitResponse{RegionError: regionErr.RequestErr}, err
			}
			return nil, err
		}
		// Rollback类型的Write表示是正确提交的事务
		if write != nil && write.Kind != mvcc.WriteKindRollback && write.StartTS == req.StartVersion {
			return &kvrpcpb.CommitResponse{}, nil
		}
		lock, err := mvccTxn.GetLock(key)
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				return &kvrpcpb.CommitResponse{RegionError: regionErr.RequestErr}, nil
			}
			return nil, err
		}
		if lock == nil || lock.Ts != req.StartVersion {

			return &kvrpcpb.CommitResponse{
				Error: &kvrpcpb.KeyError{
					Retryable: "true",
				},
			}, err
		}
		mvccTxn.PutWrite(key, req.CommitVersion, &mvcc.Write{
			StartTS: req.StartVersion,
			Kind:    lock.Kind,
		})
		mvccTxn.DeleteLock(key)
	}

	// 写入数据
	err = server.storage.Write(req.Context, mvccTxn.Writes())
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			return &kvrpcpb.CommitResponse{RegionError: regionErr.RequestErr}, err
		}
		return nil, err
	}
	return &kvrpcpb.CommitResponse{}, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			return &kvrpcpb.ScanResponse{RegionError: regionErr.RequestErr}, err
		}
		return nil, err
	}
	defer reader.Close()

	mvccTxn := mvcc.NewMvccTxn(reader, req.Version)
	scanner := mvcc.NewScanner(req.StartKey, mvccTxn)
	defer scanner.Close()
	resp := &kvrpcpb.ScanResponse{}
	var kvs []*kvrpcpb.KvPair
	for i := 0; i < int(req.Limit); {
		key, value, err := scanner.Next()
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				return &kvrpcpb.ScanResponse{RegionError: regionErr.RequestErr}, err
			}
		}
		if key == nil {
			break
		}
		lock, err := mvccTxn.GetLock(key)
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				return &kvrpcpb.ScanResponse{RegionError: regionErr.RequestErr}, err
			}
		}
		if lock != nil && lock.Ts <= req.Version {
			kvs = append(kvs, &kvrpcpb.KvPair{
				Error: &kvrpcpb.KeyError{Locked: &kvrpcpb.LockInfo{
					PrimaryLock: lock.Primary,
					LockVersion: lock.Ts,
					Key:         key,
					LockTtl:     lock.Ttl,
				}},
				Key: key,
			})
			i++
			continue
		}
		if value != nil {
			kvs = append(kvs, &kvrpcpb.KvPair{
				Key:   key,
				Value: value,
			})
			i++
		}

	}
	resp.Pairs = kvs
	return resp, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			return &kvrpcpb.CheckTxnStatusResponse{RegionError: regionErr.RequestErr}, err
		}
		return nil, err
	}
	defer reader.Close()

	mvccTxn := mvcc.NewMvccTxn(reader, req.LockTs)
	// 1. 检查Primary 是否存在Write，Write的开始时间戳与req.LockTs相等
	write, ts, err := mvccTxn.CurrentWrite(req.PrimaryKey)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			return &kvrpcpb.CheckTxnStatusResponse{RegionError: regionErr.RequestErr}, err
		}
		return nil, err
	}
	resp := &kvrpcpb.CheckTxnStatusResponse{}
	// 1.1 如果WriteKind不是WriteKindRollback则说明已经被commit
	if write != nil {
		if write.Kind != mvcc.WriteKindRollback {
			resp.CommitVersion = ts
		}
		return resp, nil
	}
	// 2. 检查lock是否存在
	lock, err := mvccTxn.GetLock(req.PrimaryKey)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			return &kvrpcpb.CheckTxnStatusResponse{RegionError: regionErr.RequestErr}, err
		}
		return nil, err
	}
	// 2.1 lock不存在，说明primary key已经被回滚，创建一个WriteKindRollKind
	if lock == nil {
		mvccTxn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
			StartTS: req.LockTs,
			Kind:    mvcc.WriteKindRollback,
		})
		err := server.storage.Write(req.Context, mvccTxn.Writes())
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				return &kvrpcpb.CheckTxnStatusResponse{RegionError: regionErr.RequestErr}, err
			}
			return nil, err
		}
		resp.Action = kvrpcpb.Action_LockNotExistRollback
		return resp, nil
	}
	// lock不为空，检查lock是否超时，如果超时则溢出lock和value，创建一个RollBack

	if mvcc.PhysicalTime(lock.Ts)+lock.Ttl <= mvcc.PhysicalTime(req.CurrentTs) {
		mvccTxn.DeleteLock(req.PrimaryKey)
		mvccTxn.DeleteValue(req.PrimaryKey)
		mvccTxn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
			StartTS: req.LockTs,
			Kind:    mvcc.WriteKindRollback,
		})
		err := server.storage.Write(req.Context, mvccTxn.Writes())
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				return &kvrpcpb.CheckTxnStatusResponse{RegionError: regionErr.RequestErr}, err
			}
			return nil, err
		}
		resp.Action = kvrpcpb.Action_TTLExpireRollback
	}

	return resp, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			return &kvrpcpb.BatchRollbackResponse{RegionError: regionErr.RequestErr}, err
		}
		return nil, err
	}
	defer reader.Close()
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)
	mvccTxn := mvcc.NewMvccTxn(reader, req.StartVersion)
	resp := &kvrpcpb.BatchRollbackResponse{}
	for i := 0; i < len(req.Keys); i++ {
		key := req.Keys[i]
		write, _, err := mvccTxn.CurrentWrite(key)
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				return &kvrpcpb.BatchRollbackResponse{RegionError: regionErr.RequestErr}, err
			}
			return nil, err
		}
		if write != nil {
			if write.Kind == mvcc.WriteKindRollback {
				continue
			} else {
				resp.Error = &kvrpcpb.KeyError{Abort: "true"}
				return resp, nil
			}
		}
		// 获取 Lock，如果 Lock 被清除或者 Lock 不是当前事务的 Lock，则中止操作
		// 这个时候说明 key 被其他事务占用
		// 否则的话移除 Lock、删除 Value，写入 WriteKindRollback 的 Write
		lock, err := mvccTxn.GetLock(key)
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				return &kvrpcpb.BatchRollbackResponse{RegionError: regionErr.RequestErr}, err
			}
			return nil, err
		}
		if lock == nil || lock.Ts != req.StartVersion {
			mvccTxn.PutWrite(key, req.StartVersion, &mvcc.Write{
				StartTS: req.StartVersion,
				Kind:    mvcc.WriteKindRollback,
			})
			continue
		}
		mvccTxn.DeleteLock(key)
		mvccTxn.DeleteValue(key)
		mvccTxn.PutWrite(key, req.StartVersion, &mvcc.Write{
			StartTS: req.StartVersion,
			Kind:    mvcc.WriteKindRollback,
		})
	}
	err = server.storage.Write(req.Context, mvccTxn.Writes())
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			return &kvrpcpb.BatchRollbackResponse{RegionError: regionErr.RequestErr}, err
		}
		return nil, err
	}
	return resp, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			return &kvrpcpb.ResolveLockResponse{RegionError: regionErr.RequestErr}, err
		}
		return nil, err
	}
	iter := reader.IterCF(engine_util.CfLock)
	defer reader.Close()
	defer iter.Close()
	var keys [][]byte
	for ; iter.Valid(); iter.Next() {
		item := iter.Item()
		value, err := item.ValueCopy(nil)
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				return &kvrpcpb.ResolveLockResponse{RegionError: regionErr.RequestErr}, err
			}
			return nil, err
		}
		lock, err := mvcc.ParseLock(value)
		if err != nil {
			return &kvrpcpb.ResolveLockResponse{}, nil
		}
		if lock.Ts == req.StartVersion {
			key := item.KeyCopy(nil)
			keys = append(keys, key)
		}
	}
	if len(keys) == 0 {
		return &kvrpcpb.ResolveLockResponse{}, nil
	}
	response := &kvrpcpb.ResolveLockResponse{}
	if req.CommitVersion == 0 {
		rollback, err := server.KvBatchRollback(nil, &kvrpcpb.BatchRollbackRequest{
			Context:      req.Context,
			StartVersion: req.StartVersion,
			Keys:         keys,
		})
		response.Error, response.RegionError = rollback.Error, rollback.RegionError
		return response, err
	} else {
		rollback, err := server.KvCommit(nil, &kvrpcpb.CommitRequest{
			Context:       req.Context,
			StartVersion:  req.StartVersion,
			Keys:          keys,
			CommitVersion: req.CommitVersion,
		})
		response.Error, response.RegionError = rollback.Error, rollback.RegionError
		return response, err
	}
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
