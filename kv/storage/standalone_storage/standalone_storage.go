package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	engines *engine_util.Engines
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	db := engine_util.CreateDB(conf.DBPath, conf.Raft)
	engines := engine_util.NewEngines(db, nil, conf.DBPath, "")
	return &StandAloneStorage{
		engines: engines,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	badgerReader := &BadgerReader{
		txn: s.engines.Kv.NewTransaction(false),
	}
	return badgerReader, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	//util.go中已经完成了对badger数据库操作的封装，直接使用即可，write_batch.go是批量操作的二次封装
	wb := &engine_util.WriteBatch{}
	for _, modify := range batch {
		switch modify.Data.(type) {
		case storage.Put:
			wb.SetCF(modify.Cf(), modify.Key(), modify.Value())
		case storage.Delete:
			wb.DeleteCF(modify.Cf(), modify.Key())
		}
	}
	err := wb.WriteToDB(s.engines.Kv)
	if err != nil {
		return err
	}
	return nil
}

// wanghao:实现StorageReader的结构体
type BadgerReader struct {
	txn *badger.Txn
}

func (br *BadgerReader) GetCF(cf string, key []byte) ([]byte, error) {
	//接口GetCF要求key不存在时，返回nil，不返回错误
	value, err := engine_util.GetCFFromTxn(br.txn, cf, key)
	if err != nil && err != badger.ErrKeyNotFound{
		return nil, err
	}
	return value, nil
}

func (br *BadgerReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, br.txn)
}

func (br *BadgerReader) Close() {
	br.txn.Discard()
}
