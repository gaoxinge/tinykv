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
	r, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	value, err := r.GetCF(req.GetCf(), req.GetKey())
	response := &kvrpcpb.RawGetResponse{Value: value}
	if value == nil && err == nil {
		response.NotFound = true
	}
	return response, err
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	put := storage.Put{Key: req.GetKey(), Value: req.GetValue(), Cf: req.GetCf()}
	modify := storage.Modify{Data: put}
	err := server.storage.Write(nil, []storage.Modify{modify})
	return &kvrpcpb.RawPutResponse{}, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	delete := storage.Delete{Key: req.GetKey(), Cf: req.GetCf()}
	modify := storage.Modify{Data: delete}
	err := server.storage.Write(nil, []storage.Modify{modify})
	return &kvrpcpb.RawDeleteResponse{}, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	r, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	cf := req.GetCf()
	limit := int(req.GetLimit())

	kvs := make([]*kvrpcpb.KvPair, 0)
	iter := r.IterCF(cf)
	defer iter.Close()
	for iter.Seek([]byte{}); iter.Valid(); iter.Next() {
		if len(kvs) >= limit {
			break
		}
		item := iter.Item()
		k := item.Key()
		v, err := item.Value()
		if err != nil {
			break
		}
		kvs = append(kvs, &kvrpcpb.KvPair{Key: k, Value: v})
	}
	return &kvrpcpb.RawScanResponse{Kvs: kvs}, err
}
