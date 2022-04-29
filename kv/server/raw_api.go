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
	//return nil, nil
	resp := &kvrpcpb.RawGetResponse{}
	reader, err := server.storage.Reader(req.Context)
	defer reader.Close()
	if err != nil {
		resp.Error = err.Error()
		return resp, err
	}
	value, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		resp.Error = err.Error()
		return resp, err
	}
	// key not found
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
	//return nil, nil
	resp := &kvrpcpb.RawPutResponse{}
	modify := []storage.Modify{
		{
			storage.Put{
				Key:   req.Key,
				Value: req.Value,
				Cf:    req.Cf,
			},
		},
	}
	err := server.storage.Write(req.Context, modify)
	if err != nil {
		resp.Error = err.Error()
	}
	return resp, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	//return nil, nil
	resp := &kvrpcpb.RawDeleteResponse{}
	modify := []storage.Modify{
		{
			storage.Delete{
				Key: req.Key,
				Cf:  req.Cf,
			},
		},
	}
	err := server.storage.Write(req.Context, modify)
	if err != nil {
		resp.Error = err.Error()
	}
	return resp, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	//return nil, nil
	resp := &kvrpcpb.RawScanResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		resp.Error = err.Error()
		return resp, err
	}
	defer reader.Close()
	iter := reader.IterCF(req.Cf)
	defer iter.Close()

	limit := req.Limit

	for iter.Seek(req.StartKey); iter.Valid() && limit > 0; iter.Next() {
		item := iter.Item()
		value, err := item.ValueCopy(nil)
		if err != nil {
			resp.Error = err.Error()
			resp.Kvs = nil
			return resp, err
		}
		resp.Kvs = append(resp.Kvs, &kvrpcpb.KvPair{
			Key:   item.KeyCopy(nil),
			Value: value,
		})
		limit--
	}

	return resp, nil
}
