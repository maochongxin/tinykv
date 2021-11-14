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
	localStorage *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {

	return &StandAloneStorage{
		localStorage: &badger.DB{},
	}
}

func (s *StandAloneStorage) Start() error {

	return nil
}

func (s *StandAloneStorage) Stop() error {
	err := s.localStorage.Close()
	if err != nil {
		return err
	}
	return nil
}

type reader struct {
	txn *badger.Txn
}

func (r *reader) GetCF(cf string, key []byte) ([]byte, error)  {
	return engine_util.GetCFFromTxn(r.txn, cf, key)
}

func (r *reader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.txn)
}

func (r *reader) Close() {
	r.txn.Discard()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	return &reader{
		txn: s.localStorage.NewTransaction(false),
	}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	txn := s.localStorage.NewTransaction(true)
	defer txn.Discard()
	for _, entry := range batch {
		err := txn.Set(entry.Key(), entry.Value())
		if err != nil {
			return err
		}
	}
	err := txn.Commit()
	if err != nil {
		return err
	}
	return nil
}
