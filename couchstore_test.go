package couchstore

import (
	"fmt"
	"os"
	"testing"
)

func TestCouchstoreCrud(t *testing.T) {
	os.RemoveAll("test.couch")

	cst, err := Open("test.couch", OpenFlagCreate)
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
		t.Fatalf("expected %v, got %v", COUCHSTORE_ERROR_DOC_NOT_FOUND, err)
	}

	// put a new key
	doc2, err := NewDoc([]byte("key1"), nil, []byte("value1"))
	if err != nil {
		t.Error(err)
	}
	err = cst.Set(doc2)
	if err != nil {
		t.Error(err)
	}

	err = cst.Commit()
	if err != nil {
		t.Error(err)
	}

	// lookup that key
	doc3, err := NewDoc([]byte("key1"), nil, nil)
	if err != nil {
		t.Error(err)
	}
	err = cst.Get(doc3)
	if err != nil {
		t.Error(err)
	}
	if string(doc3.Body()) != "value1" {
		t.Errorf("expected value1, got %s", doc3.Body())
	}

	// update it
	doc4, err := NewDoc([]byte("key1"), nil, []byte("value1-updated"))
	if err != nil {
		t.Error(err)
	}
	err = cst.Set(doc4)
	if err != nil {
		t.Error(err)
	}

	// look it up again
	doc5, err := NewDoc([]byte("key1"), nil, nil)
	if err != nil {
		t.Error(err)
	}
	err = cst.Get(doc5)
	if err != nil {
		t.Error(err)
	}
	if string(doc5.Body()) != "value1-updated" {
		t.Errorf("expected value1-updated, got %s", doc5.Body())
	}

	// delete it
	doc6, err := NewDoc([]byte("key1"), nil, nil)
	if err != nil {
		t.Error(err)
	}
	err = cst.Delete(doc6)
	if err != nil {
		t.Error(err)
	}

	err = cst.Commit()
	if err != nil {
		t.Error(err)
	}

	// look it up again
	doc7, err := NewDoc([]byte("key1"), nil, nil)
	if err != nil {
		t.Error(err)
	}
	err = cst.Get(doc7)
	if err != nil {
		t.Error(err)
	}
	if string(doc7.Body()) != "" {
		t.Errorf("expected value1-updated, got %s", doc7.Body())
	}

	// check the db info at the end
	kvInfo, err := cst.Info()
	if err != nil {
		t.Error(err)
	}

	if kvInfo.DocCount() != 0 {
		t.Fatalf("Incorrect doc count %v", kvInfo.DocCount())
	}
}

func TestCouchstoreCompact(t *testing.T) {
	defer os.RemoveAll("test.couch")
	defer os.RemoveAll("test-compacted.couch")

	cst, err := Open("test.couch", OpenFlagCreate)
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

	err = cst.Compact("test-compacted.couch")
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
