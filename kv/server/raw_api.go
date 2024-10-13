package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	resp := &kvrpcpb.RawGetResponse{}

	storageReader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer storageReader.Close()

	value, err := storageReader.GetCF(req.Cf, req.Key)
	if err != nil {
		return nil, err
	}

	if value == nil {
		resp.NotFound = true
	}
	resp.Value = value
	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	resp := &kvrpcpb.RawPutResponse{}
	//构造[]stoarge.Modify变量，提供给write方法使用
	var batch []storage.Modify
	put := storage.Put{
		Key: req.Key,
		Value: req.Value,
		Cf: req.Cf,
	}
	modify := storage.Modify{
		Data: put,
	}
	batch = append(batch, modify)
	//调用存储层的write方法进行写操作
	err := server.storage.Write(req.Context, batch)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	resp := &kvrpcpb.RawDeleteResponse{}
	//构造[]stoarge.Modify变量，提供给write方法使用
	var batch []storage.Modify
	delete := storage.Delete{
		Key: req.Key,
		Cf: req.Cf,
	}
	modify := storage.Modify{
		Data: delete,
	}
	batch = append(batch, modify)
	//调用存储层的write方法进行写操作
	err := server.storage.Write(req.Context, batch)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF

	resp := &kvrpcpb.RawScanResponse{}

	storageReader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer storageReader.Close()

	dbIterator := storageReader.IterCF(req.Cf)
	defer dbIterator.Close()
	var count uint32 = 0

	for dbIterator.Seek(req.StartKey); dbIterator.Valid(); dbIterator.Next() {
		if count >= req.GetLimit() {
			break
		}

		item := dbIterator.Item()
		k := item.Key()
		v, err := item.Value()
		if err != nil {
			return nil ,err
		}
		kvPair := &kvrpcpb.KvPair{
			Key: k,
			Value: v,
		}
		resp.Kvs = append(resp.Kvs, kvPair)

		count++
	  }

	return resp, nil
}
