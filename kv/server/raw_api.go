package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	rep := &kvrpcpb.RawGetResponse{}
	reader, _ := server.storage.Reader(req.Context)
	defer reader.Close()
	value, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		rep.Error = err.Error()
		rep.NotFound = true
		return rep, nil
	}
	if len(value) == 0 {
		rep.NotFound = true
	}
	rep.Value = value
	return rep, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	rep := &kvrpcpb.RawPutResponse{}
	batch := []storage.Modify{
		{
			Data: storage.Put{
				Key:   req.Key,
				Value: req.Value,
				Cf:    req.Cf,
			},
		},
	}
	err := server.storage.Write(req.Context, batch)
	if err != nil {
		rep.Error = err.Error()
	}
	return rep, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	rep := &kvrpcpb.RawDeleteResponse{}
	batch := []storage.Modify{
		{
			Data: storage.Put{
				Key: req.Key,
				Cf:  req.Cf,
			},
		},
	}
	err := server.storage.Write(req.Context, batch)
	if err != nil {
		rep.Error = err.Error()
	}
	return rep, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	rep := &kvrpcpb.RawScanResponse{}
	reader, _ := server.storage.Reader(req.Context)
	defer reader.Close()
	it := reader.IterCF(req.Cf)
	defer it.Close()
	it.Seek(req.StartKey)
	for i := uint32(0); it.Valid() && i < req.Limit; it.Next() {
		item := it.Item()
		value, err := item.Value()
		if err != nil {
			log.Warn("seek value error")
		}
		rep.Kvs = append(rep.Kvs, &kvrpcpb.KvPair{
			Key:   item.Key(),
			Value: value,
		})
		i++
	}
	return rep, nil
}
