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
	rsp := &kvrpcpb.RawGetResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		rsp.Error = err.Error()
		return rsp, nil
	}

	val, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		rsp.Error = err.Error()
		return rsp, nil
	}

	if val == nil {
		rsp.NotFound = true
		return rsp, nil
	}
	rsp.Value = val
	return rsp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	rsp := &kvrpcpb.RawPutResponse{}
	err := server.storage.Write(req.Context, []storage.Modify{
		{
			Data: storage.Put{
				Key:   req.Key,
				Value: req.Value,
				Cf:    req.Cf,
			},
		},
	})
	if err != nil {
		rsp.Error = err.Error()
	}
	return rsp, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	rsp := &kvrpcpb.RawDeleteResponse{}
	err := server.storage.Write(req.Context, []storage.Modify{
		{
			Data: storage.Put{
				Key: req.Key,
				Cf:  req.Cf,
			},
		},
	})

	if err != nil {
		rsp.Error = err.Error()
	}
	return rsp, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	rsp := &kvrpcpb.RawScanResponse{}
	reader, err := server.storage.Reader(req.Context)
	defer reader.Close()
	if err != nil {
		rsp.Error = err.Error()
		return rsp, nil
	}
	dbIter := reader.IterCF(req.Cf)
	defer dbIter.Close()
	dbIter.Seek(req.StartKey)

	for i := 0; dbIter.Valid() && i < int(req.Limit); i++ {
		key := dbIter.Item().Key()
		val, _ := dbIter.Item().Value()
		rsp.Kvs = append(rsp.Kvs, &kvrpcpb.KvPair{
			Key:   key,
			Value: val,
		})
		dbIter.Next()
	}
	return rsp, nil
}
