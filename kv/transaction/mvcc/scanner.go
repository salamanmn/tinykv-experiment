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
	NextUserKey []byte
	mvccTxn 	*MvccTxn
	Iterator 	engine_util.DBIterator
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	scan := &Scanner{
		NextUserKey: startKey,
		mvccTxn: txn,
		Iterator: txn.Reader.IterCF(engine_util.CfWrite),
	}
	return scan
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	scan.Iterator.Close()
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	//获取NextUserKey最新版本的值
	scan.Iterator.Seek(EncodeKey(scan.NextUserKey, scan.mvccTxn.StartTS))
	if !scan.Iterator.Valid() {
		return nil, nil, nil
	}

	item := scan.Iterator.Item()
	userKey := DecodeUserKey(item.KeyCopy(nil))
	if !bytes.Equal(userKey, scan.NextUserKey) {//NextUserKey没有对应的最新write，从下一个userKey开始
		scan.NextUserKey = userKey
		return scan.Next()
	}


	//更新NextUserKey
	scan.Iterator.Next() 
	for ; scan.Iterator.Valid(); scan.Iterator.Next() {
		nextItem := scan.Iterator.Item()
		nextKey := nextItem.KeyCopy(nil)
		nextUserKey := DecodeUserKey(nextKey)
		if !bytes.Equal(nextUserKey, scan.NextUserKey) {
			scan.NextUserKey = nextUserKey
			break
		}
	}

	var returnKey   []byte = DecodeUserKey(item.KeyCopy(nil))
	var returnValue []byte = nil
	writeValue, err := item.ValueCopy(nil)
	if err != nil {
		return returnKey, nil, err
	}
	write, err := ParseWrite(writeValue)
	if err != nil {
		return returnKey, nil, err
	}
	if write.Kind == WriteKindDelete {
		returnValue = nil
	} else if write.Kind == WriteKindPut {
		returnValue, err = scan.mvccTxn.Reader.GetCF(engine_util.CfDefault, EncodeKey(returnKey, write.StartTS))
		if err != nil {
			return returnKey, nil, err
		}
	}

	return returnKey, returnValue, nil
}