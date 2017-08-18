package couchstore

//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

// To run with couchstore ensure that the following env variables are set.
// export CGO_CFLAGS="-I${COUCHBASE_DIR}/couchstore/include
//                    -I${COUCHBASE_DIR}/platform/include"
//                    -I${COUCHBASE_DIR}/build/platform/include"
// export CGO_LDFLAGS="-L {COUCHBASE_DIR}/install/lib"
// export LD_LIBRARY_PATH={COUCHBASE_DIR}/install/lib

//#cgo LDFLAGS: -lcouchstore
//#include <stdlib.h>
//#include <libcouchstore/couch_db.h>
//#include <libcouchstore/couch_common.h>
import "C"

import (
	"errors"
	"fmt"
	"unsafe"
)

// CouchSt handle
type CouchSt struct {
	db *C.Db
}

type Errno int
type SeqNum uint64

func (e Errno) Error() string {
	s := errText[e]
	if s == "" {
		return fmt.Sprintf("errno %d", int(e))
	}
	return s
}

const (
	COUCHSTORE_SUCCESS                   C.couchstore_error_t = 0
	COUCHSTORE_ERROR_OPEN_FILE           Errno                = -1
	COUCHSTORE_ERROR_CORRUPT             Errno                = -2
	COUCHSTORE_ERROR_ALLOC_FAIL          Errno                = -3
	COUCHSTORE_ERROR_READ                Errno                = -4
	COUCHSTORE_ERROR_DOC_NOT_FOUND       Errno                = -5
	COUCHSTORE_ERROR_NO_HEADER           Errno                = -6
	COUCHSTORE_ERROR_WRITE               Errno                = -7
	COUCHSTORE_ERROR_HEADER_VERSION      Errno                = -8
	COUCHSTORE_ERROR_CHECKSUM_FAIL       Errno                = -9
	COUCHSTORE_ERROR_INVALID_ARGUMENTS   Errno                = -10
	COUCHSTORE_ERROR_NO_SUCH_FILE        Errno                = -11
	COUCHSTORE_ERROR_CANCEL              Errno                = -12
	COUCHSTORE_ERROR_REDUCTION_TOO_LARGE Errno                = -13
	COUCHSTORE_ERROR_REDUCER_FAILURE     Errno                = -14
	COUCHSTORE_ERROR_FILE_CLOSED         Errno                = -15
	COUCHSTORE_ERROR_DB_NO_LONGER_VALID  Errno                = -16
)

var errText = map[Errno]string{
	-1:  "COUCHSTORE_ERROR_OPEN_FILE",
	-2:  "COUCHSTORE_ERROR_CORRUPT",
	-3:  "COUCHSTORE_ERROR_ALLOC_FAIL",
	-4:  "COUCHSTORE_ERROR_READ",
	-5:  "COUCHSTORE_ERROR_DOC_NOT_FOUND",
	-6:  "COUCHSTORE_ERROR_NO_HEADER",
	-7:  "COUCHSTORE_ERROR_WRITE",
	-8:  "COUCHSTORE_ERROR_HEADER_VERSION",
	-9:  "COUCHSTORE_ERROR_CHECKSUM_FAIL",
	-10: "COUCHSTORE_ERROR_INVALID_ARGUMENTS",
	-11: "COUCHSTORE_ERROR_NO_SUCH_FILE",
	-12: "COUCHSTORE_ERROR_CANCEL",
	-13: "COUCHSTORE_ERROR_REDUCTION_TOO_LARGE",
	-14: "COUCHSTORE_ERROR_REDUCER_FAILURE",
	-15: "COUCHSTORE_ERROR_FILE_CLOSED",
	-16: "COUCHSTORE_ERROR_DB_NO_LONGER_VALID",
}

type Doc struct {
	doc  C.Doc
	info C.DocInfo
}

func NewDoc(key, meta, body []byte) (*Doc, error) {
	rv := Doc{}

	if len(key) != 0 {
		rv.doc.id.buf = (*C.char)(unsafe.Pointer(&key[0]))
		rv.doc.id.size = C.size_t(len(key))
		rv.info.id = rv.doc.id
	}

	if len(meta) != 0 {
		rv.info.rev_meta.buf = (*C.char)(unsafe.Pointer(&meta[0]))
		rv.info.rev_meta.size = C.size_t(len(meta))
	}

	if len(body) != 0 {
		rv.doc.data.buf = (*C.char)(unsafe.Pointer(&body[0]))
		rv.doc.data.size = C.size_t(len(body))
	}

	return &rv, nil
}

func (d *Doc) Key() []byte {
	return C.GoBytes(unsafe.Pointer(d.doc.id.buf), C.int(d.doc.id.size))
}

func (d *Doc) Meta() []byte {
	return C.GoBytes(unsafe.Pointer(d.info.rev_meta.buf), C.int(d.info.rev_meta.size))
}

func (d *Doc) Body() []byte {
	return C.GoBytes(unsafe.Pointer(d.doc.data.buf), C.int(d.doc.data.size))
}

type CouchstoreOpenFlags C.couchstore_open_flags

const (
	// Create a new empty .couch file if file doesn't exist.
	OpenFlagCreate CouchstoreOpenFlags = C.COUCHSTORE_OPEN_FLAG_CREATE

	// Open the database in read only mode
	OpenFlagRDOnly CouchstoreOpenFlags = C.COUCHSTORE_OPEN_FLAG_RDONLY

	/**
	 * Require the database to use the legacy CRC.
	 * This forces the disk_version flag to be 11 and is only valid for new files
	 * and existing version 11 files.
	 * When excluded the correct CRC is automatically chosen for existing files.
	 * When excluded the latest file version is always used for new files.
	 */
	OpenFlagLegacyCRC CouchstoreOpenFlags = C.COUCHSTORE_OPEN_WITH_LEGACY_CRC

	/**
	 * Open the database file without using an IO buffer
	 *
	 * This prevents the FileOps that are used in from being
	 * wrapped by the buffered file operations. This will
	 * *usually* result in performance degradation and is
	 * primarily intended for testing purposes.
	 */
	OpenFlagUnbuffered CouchstoreOpenFlags = C.COUCHSTORE_OPEN_FLAG_UNBUFFERED

	/**
	 * Customize IO buffer configurations.
	 *
	 * This specifies the capacity of a read buffer and its count.
	 * The first 4 bits are for the capacity, that will be calculated as:
	 *     1KB * 1 << (N-1)
	 * And the next 4 bits are for the count:
	 *     8 * 1 << (N-1)
	 * Note that all zeros represent the default setting.
	 */
	OpenFlagWithCustomBuffer CouchstoreOpenFlags = C.COUCHSTORE_OPEN_WITH_CUSTOM_BUFFER

	/**
	 * Customize B+tree node size.
	 *
	 * This specifies the size of B+tree node.
	 * The first 4 bits represents the size of key-pointer
	 * (i.e., intermediate) nodes in KB, and the next 4 bits denotes
	 * the size of key-value (i.e., leaf) nodes in KB.
	 * Note that all zeros represent the default setting,
	 * 1279 (0x4ff) bytes.
	 */
	OpenFlagCustomNodeSize CouchstoreOpenFlags = C.COUCHSTORE_OPEN_WITH_CUSTOM_NODESIZE

	/**
	 * Enable periodic sync().
	 *
	 * Automatically perform a sync() call after every N bytes written.
	 *
	 * When writing large amounts of data (e.g during compaction), read
	 * latency can be adversely affected if a single sync() is made at the
	 * end of writing all the data; as the IO subsystem has a large amount
	 * of outstanding writes to flush to disk. By issuing periodic syncs
	 * the affect on read latency can be signifcantly reduced.
	 *
	 * Encoded as a power-of-2 KB value, ranging from 1KB .. 1TB (5 bits):
	 *     1KB * << (N-1)
	 *
	 * A value of N=0 specifies that automatic fsync is disabled.
	 */
	OpenFlagPeriodicSync CouchstoreOpenFlags = C.COUCHSTORE_OPEN_WITH_PERIODIC_SYNC
)

func Open(filename string, openFlags CouchstoreOpenFlags) (*CouchSt, error) {
	var s *C.Db

	dbname := C.CString(filename)
	defer C.free(unsafe.Pointer(dbname))

	rv := C.couchstore_open_db(dbname, C.couchstore_open_flags(openFlags), &s)

	if rv != 0 {
		return nil, errors.New(Errno(rv).Error())
	}

	if s == nil {
		return nil, errors.New("couchstore succeeded without returning a database")
	}

	return &CouchSt{s}, nil
}

func (s *CouchSt) Set(doc *Doc) error {
	rv := C.couchstore_save_document(s.db, &doc.doc, &doc.info, 0)
	if rv != 0 {
		return Errno(rv)
	}
	return nil
}

func (s *CouchSt) Get(doc *Doc) error {
	var docOut *C.Doc
	rv := C.couchstore_open_document(s.db, unsafe.Pointer(doc.doc.id.buf),
		doc.doc.id.size, &docOut, 0)

	if rv != 0 {
		return Errno(rv)
	}

	doc.doc = *docOut
	return nil
}

func (s *CouchSt) Delete(doc *Doc) error {
	doc.info.deleted = C.int(1)
	return s.Set(doc)
}

func (s *CouchSt) Compact(newfilename string) error {
	f := C.CString(newfilename)
	defer C.free(unsafe.Pointer(f))

	rv := C.couchstore_compact_db(s.db, f)
	if rv != 0 {
		return Errno(rv)
	}
	return nil
}

func (s *CouchSt) Commit() error {
	rv := C.couchstore_commit(s.db)
	if rv != 0 {
		return Errno(rv)
	}
	return nil
}

func (s *CouchSt) Close() error {
	rv := C.couchstore_close_file(s.db)
	if rv != 0 {
		return Errno(rv)
	}
	return nil
}

type DbInfo struct {
	info C.DbInfo
}

func (dbi *DbInfo) LastSeqNum() SeqNum {
	return SeqNum(dbi.info.last_sequence)
}

func (dbi *DbInfo) SpaceUsed() uint64 {
	return uint64(dbi.info.space_used)
}

func (dbi *DbInfo) FileSize() uint64 {
	return uint64(dbi.info.file_size)
}

func (dbi *DbInfo) DocCount() uint64 {
	return uint64(dbi.info.doc_count)
}

func (dbi *DbInfo) DeletedCount() uint64 {
	return uint64(dbi.info.deleted_count)
}

func (s *CouchSt) Info() (*DbInfo, error) {
	var info DbInfo
	rv := C.couchstore_db_info(s.db, &info.info)
	if rv != 0 {
		return nil, Errno(rv)
	}
	return &info, nil
}
