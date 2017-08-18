package couchstore

import (
	"fmt"
	"os"
	"testing"
)

func TestCouchstoreCrud(t *testing.T) {
	defer os.RemoveAll("test")

	cst, err := Open("test", OpenFlagCreate)
	if err != nil {
		t.Fatal(err)
	}
	defer cst.Close()

	// get a non-existant key
	doc, err := NewDoc([]byte("doesnotexist"), nil, nil)
	if err != nil {
		t.Error(err)
	}
	err = cst.Get(doc)
	if err != COUCHSTORE_ERROR_DOC_NOT_FOUND {
		t.Errorf("expected %v, got %v", COUCHSTORE_ERROR_DOC_NOT_FOUND, err)
	}

	// put a new key
	doc, err = NewDoc([]byte("key1"), nil, []byte("value1"))
	if err != nil {
		t.Error(err)
	}
	err = cst.Set(doc)
	if err != nil {
		t.Error(err)
	}

	// lookup that key
	doc, err = NewDoc([]byte("key1"), nil, nil)
	if err != nil {
		t.Error(err)
	}
	err = cst.Get(doc)
	if err != nil {
		t.Error(err)
	}
	if string(doc.Body()) != "value1" {
		t.Errorf("expected value1, got %s", doc.Body())
	}

	// update it
	doc, err = NewDoc([]byte("key1"), nil, []byte("value1-updated"))
	if err != nil {
		t.Error(err)
	}
	err = cst.Set(doc)
	if err != nil {
		t.Error(err)
	}

	// look it up again
	doc, err = NewDoc([]byte("key1"), nil, nil)
	if err != nil {
		t.Error(err)
	}
	err = cst.Get(doc)
	if err != nil {
		t.Error(err)
	}
	if string(doc.Body()) != "value1-updated" {
		t.Errorf("expected value1-updated, got %s", doc.Body())
	}

	// delete it
	doc, err = NewDoc([]byte("key1"), nil, nil)
	if err != nil {
		t.Error(err)
	}
	err = cst.Delete(doc)
	if err != nil {
		t.Error(err)
	}

	// look it up again
	doc, err = NewDoc([]byte("key1"), nil, nil)
	if err != nil {
		t.Error(err)
	}
	err = cst.Get(doc)
	if err != COUCHSTORE_ERROR_DOC_NOT_FOUND {
		t.Error(err)
	}

	// delete it again
	doc, err = NewDoc([]byte("key1"), nil, nil)
	if err != nil {
		t.Error(err)
	}
	err = cst.Delete(doc)
	if err != nil {
		t.Error(err)
	}

	// delete non-existant key
	doc, err = NewDoc([]byte("doesnotext"), nil, nil)
	if err != nil {
		t.Error(err)
	}
	err = cst.Delete(doc)
	if err != nil {
		t.Error(err)
	}

	// check the db info at the end
	kvInfo, err := cst.Info()
	if err != nil {
		t.Error(err)
	}
	if kvInfo.DocCount() == 0 {
		t.Errorf("Incorrect doc count", kvInfo.DocCount())
	}
}

func TestCouchstoreCompact(t *testing.T) {
	defer os.RemoveAll("test")
	defer os.RemoveAll("test-compacted")

	cst, err := Open("test", OpenFlagCreate)
	if err != nil {
		t.Fatal(err)
	}
	defer cst.Close()

	for i := 0; i < 1000; i++ {
		doc, err := NewDoc([]byte(fmt.Sprintf("key-%d", i)), nil, []byte("value1"))
		if err != nil {
			t.Error(err)
		}
		err = cst.Set(doc)
		if err != nil {
			t.Error(err)
		}
	}

	err = cst.Compact("test-compacted")
	if err != nil {
		t.Error(err)
	}

	for i := 0; i < 1000; i++ {
		doc, _ := NewDoc([]byte(fmt.Sprintf("key-%d", i)), nil, nil)
		err = cst.Get(doc)
		if err != nil {
			t.Error(err)
		}
	}
}
