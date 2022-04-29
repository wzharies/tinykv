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
	Db   *badger.DB
	Conf *config.Config
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	storage := StandAloneStorage{Db: nil, Conf: conf}
	return &storage
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	options := badger.DefaultOptions
	options.Dir = s.Conf.DBPath
	options.ValueDir = s.Conf.DBPath

	db, err := badger.Open(options)
	if err != nil {
		return err
	}
	s.Db = db
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	//return nil
	return s.Db.Close()
}

type StandAloneStorageReader struct {
	Txn *badger.Txn
}

func NewStandAloneStorageReader(db *badger.DB) StandAloneStorageReader {
	txn := db.NewTransaction(false)
	reader := StandAloneStorageReader{Txn: txn}
	return reader
}

func (r StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	value, err := engine_util.GetCFFromTxn(r.Txn, cf, key)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return nil, nil
		} else {
			return nil, err
		}
	}
	return value, nil
}

func (r StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.Txn)
}

func (r StandAloneStorageReader) Close() {
	// read only, so just discard
	r.Txn.Discard()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	//return nil, nil
	return NewStandAloneStorageReader(s.Db), nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	//return nil
	writeBatch := &engine_util.WriteBatch{}
	for _, modify := range batch {
		cf := modify.Cf()
		key := modify.Key()
		value := modify.Value()
		if value == nil {
			writeBatch.DeleteCF(cf, key)
		} else {
			writeBatch.SetCF(cf, key, value)
		}
	}
	return writeBatch.WriteToDB(s.Db)
}
