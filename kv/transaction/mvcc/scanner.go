package mvcc

import (
	"bytes"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	key []byte
	ended bool
	txn * MvccTxn
	iter engine_util.DBIterator
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	return &Scanner{
		key:   startKey,
		ended: false,
		txn:   txn,
		iter:  txn.Reader.IterCF(engine_util.CfWrite),
	}
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	scan.iter.Close()
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	if scan.ended {
		return nil, nil, nil
	}
	// 查找cfWrite
	findKey := scan.key
	scan.iter.Seek(EncodeKey(findKey,scan.txn.StartTS))
	if !scan.iter.Valid() {
		scan.ended = true
		return nil, nil, nil
	}
	item := scan.iter.Item()
	itemKey := item.KeyCopy(nil)
	userKey := DecodeUserKey(itemKey)
	if !bytes.Equal(userKey,findKey) {
		scan.key = userKey
		return scan.Next()
	}
	for{
		scan.iter.Next()
		if !scan.iter.Valid() {
			scan.ended = true
			break
		}
		nextKey := scan.iter.Item().KeyCopy(nil)
		nextUserKey := DecodeUserKey(nextKey)
		if !bytes.Equal(nextUserKey,findKey) {
			scan.key = nextUserKey
			break
		}
	}
	val,err := item.ValueCopy(nil)
	if err != nil {
		return findKey, nil, err
	}
	write,err := ParseWrite(val)
	if err != nil {
		return findKey, nil, err
	}
	if write.Kind == WriteKindDelete {
		return findKey,nil,nil
	}
	value, err := scan.txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(findKey, write.StartTS))
	return findKey,value, err

}
