package standalone_storage

import (
	"bytes"
	"fmt"
	"github.com/Connor1996/badger"
	"github.com/Connor1996/badger/options"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap/log"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	localStorage *badger.DB
	reader *bytes.Reader
	buffer *bytes.Buffer
	lastBackup uint64

	byteBuffer []byte
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	option := badger.Options{
		Dir:              conf.DBPath,
		ValueDir:         conf.DBPath,
		ValueLogFileSize: 1024 * 1024,
		MaxCacheSize:     1024,
		MaxTableSize: int64(conf.RegionMaxSize),
		TableBuilderOptions: options.TableBuilderOptions {
			BlockSize:       int(conf.RegionSplitSize),
			WriteBufferSize: int(conf.RegionMaxSize),
			BytesPerSecond: 1024,
		},
	}
	db, err := badger.Open(option)
	fmt.Println(err.Error())
	if err != nil {
		log.Info(err.Error())
		return nil
	}
	return &StandAloneStorage{
		localStorage: db,
	}
}

func (s *StandAloneStorage) Start() error {
	log.Info("Start")
	s.byteBuffer = make([]byte, 2048)
	s.reader = bytes.NewReader(s.byteBuffer)
	s.buffer = bytes.NewBuffer(s.byteBuffer)
	err := s.localStorage.Load(s.reader)
	if err != nil {
		return err
	}
	return nil
}

func (s *StandAloneStorage) Stop() error {
	backup, err := s.localStorage.Backup(s.buffer, s.lastBackup)
	s.lastBackup = backup
	if err != nil {
		return err
	}
	err = s.localStorage.Close()
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
