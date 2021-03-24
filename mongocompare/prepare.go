// Copyright (C) MongoDB, Inc. 2014-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package mongocompare

import (
	"context"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"github.com/mongodb/mongo-tools-common/db"
	"github.com/mongodb/mongo-tools-common/intents"
	"github.com/mongodb/mongo-tools-common/log"
	"github.com/mongodb/mongo-tools-common/util"
	"io"
	"os"
	"path/filepath"
	"strings"
)

type NilPos struct{}

func (NilPos) Pos() int64 {
	return -1
}

// writeFlusher wraps an io.Writer and adds a Flush function.
type writeFlusher interface {
	Flush() error
	io.Writer
}

// writeFlushCloser is a writeFlusher implementation which exposes
// a Close function which is implemented by calling Flush.
type writeFlushCloser struct {
	writeFlusher
}

// errorReader implements io.Reader.
type errorReader struct{}

// Read on an errorReader already returns an error.
func (errorReader) Read([]byte) (int, error) {
	return 0, os.ErrInvalid
}

// Close calls Flush.
func (bwc writeFlushCloser) Close() error {
	return bwc.Flush()
}

// realBSONFile implements the intents.file interface. It lets intents write to real BSON files
// ok disk via an embedded bufio.Writer
type realBSONFile struct {
	io.WriteCloser
	path string
	// errorWrite adds a Read() method to this object allowing it to be an
	// intent.file ( a ReadWriteOpenCloser )
	errorReader
	intent *intents.Intent
	NilPos
}

// Open is part of the intents.file interface. realBSONFiles need to have Open called before
// Read can be called
func (f *realBSONFile) Open() (err error) {
	if f.path == "" {
		// This should not occur normally. All realBSONFile's should have a path
		return fmt.Errorf("error creating BSON file without a path, namespace: %v",
			f.intent.Namespace())
	}
	err = os.MkdirAll(filepath.Dir(f.path), os.ModeDir|os.ModePerm)
	if err != nil {
		return fmt.Errorf("error creating directory for BSON file %v: %v",
			filepath.Dir(f.path), err)
	}

	f.WriteCloser, err = os.Create(f.path)
	if err != nil {
		return fmt.Errorf("error creating BSON file %v: %v", f.path, err)
	}

	return nil
}

// realMetadataFile implements intent.file, and corresponds to a Metadata file on disk
type realMetadataFile struct {
	io.WriteCloser
	path string
	errorReader
	// errorWrite adds a Read() method to this object allowing it to be an
	// intent.file ( a ReadWriteOpenCloser )
	intent *intents.Intent
	NilPos
}

// Open opens the file on disk that the intent indicates. Any directories needed are created.
func (f *realMetadataFile) Open() (err error) {
	if f.path == "" {
		return fmt.Errorf("No metadata path for %v.%v", f.intent.DB, f.intent.C)
	}
	err = os.MkdirAll(filepath.Dir(f.path), os.ModeDir|os.ModePerm)
	if err != nil {
		return fmt.Errorf("error creating directory for metadata file %v: %v",
			filepath.Dir(f.path), err)
	}

	f.WriteCloser, err = os.Create(f.path)
	if err != nil {
		return fmt.Errorf("error creating metadata file %v: %v", f.path, err)
	}
	return nil
}

// stdoutFile implements the intents.file interface. stdoutFiles are used when single collections
// are written directly (non-archive-mode) to standard out, via "--dir -"
type stdoutFile struct {
	io.Writer
	errorReader
	NilPos
}

// Open is part of the intents.file interface.
func (f *stdoutFile) Open() error {
	return nil
}

// Close is part of the intents.file interface. While we could actually close os.Stdout here,
// that's actually a bad idea. Unsetting f.File here will cause future Writes to fail, which
// is all we want.
func (f *stdoutFile) Close() error {
	f.Writer = nil
	return nil
}

// shouldSkipSystemNamespace returns true when a namespace (database +
// collection name) match certain reserved system namespaces that must
// not be dumped.
func shouldSkipSystemNamespace(dbName, collName string) bool {
	// ignore <db>.system.* except for admin; ignore other specific
	// collections in config and admin databases used for 3.6 features.
	switch dbName {
	case "admin":
		if collName == "system.keys" {
			return true
		}
	case "config":
		if collName == "transactions" || collName == "system.sessions" || collName == "transaction_coordinators" || collName == "system.indexBuilds" {
			return true
		}
	default:
		if collName == "system.js" {
			return false
		}
		if strings.HasPrefix(collName, "system.") {
			return true
		}
	}

	// Skip over indexes since they are also listed in system.namespaces in 2.6 or earlier
	if strings.Contains(collName, "$") && !strings.Contains(collName, ".oplog.$") {
		return true
	}

	return false
}

// shouldSkipCollection returns true when a collection name is excluded
// by the mongodump options.
func (compare *MongoCompare) shouldSkipCollection(colName string) bool {
	for _, excludedCollection := range compare.OutputOptions.ExcludedCollections {
		if colName == excludedCollection {
			return true
		}
	}
	for _, excludedCollectionPrefix := range compare.OutputOptions.ExcludedCollectionPrefixes {
		if strings.HasPrefix(colName, excludedCollectionPrefix) {
			return true
		}
	}
	return false
}

// outputPath creates a path for the collection to be written to (sans file extension).
func (compare *MongoCompare) outputPath(dbName, colName string) string {
	var root string
	if compare.OutputOptions.Out == "" {
		root = "compare"
	} else {
		root = compare.OutputOptions.Out
	}

	// Encode a new output path for collection names that would result in a file name greater
	// than 255 bytes long. This includes the longest possible file extension: .metadata.json.gz
	// The new format is <truncated-url-encoded-collection-name>%24<collection-name-hash-base64>
	// where %24 represents a $ symbol delimiter (e.g. aVeryVery...VeryLongName%24oPpXMQ...).
	escapedColName := util.EscapeCollectionName(colName)
	if len(escapedColName) > 238 {
		colNameTruncated := escapedColName[:208]
		colNameHashBytes := sha1.Sum([]byte(colName))
		colNameHashBase64 := base64.RawURLEncoding.EncodeToString(colNameHashBytes[:])

		// First 208 bytes of col name + 3 bytes delimiter + 27 bytes base64 hash = 238 bytes max.
		escapedColName = colNameTruncated + "%24" + colNameHashBase64
	}

	return filepath.Join(root, dbName, escapedColName)
}


// CreateCollectionIntent builds an intent for a given collection and
// puts it into the intent manager.
func (compare *MongoCompare) CreateCollectionIntent(dbName, colName string) error {
	if compare.shouldSkipCollection(colName) {
		log.Logvf(log.DebugLow, "skipping dump of %v.%v, it is excluded", dbName, colName)
		return nil
	}

	session, err := compare.SrcSessionProvider.GetSession()
	if err != nil {
		return err
	}

	collOptions, err := db.GetCollectionInfo(session.Database(dbName).Collection(colName))
	if err != nil {
		return fmt.Errorf("error getting collection options: %v", err)
	}

	intent, err := compare.NewIntentFromOptions(dbName, collOptions)
	if err != nil {
		return err
	}

	compare.manager.Put(intent)
	return nil
}

func (compare *MongoCompare) NewIntentFromOptions(dbName string, ci *db.CollectionInfo) (*intents.Intent, error) {
	intent := &intents.Intent{
		DB:      dbName,
		C:       ci.Name,
		Options: ci.Options,
	}

	// Populate the intent with the collection UUID or the empty string
	intent.UUID = ci.GetUUID()

	// Setup output location
	if compare.OutputOptions.Out == "-" { // regular standard output
		intent.BSONFile = &stdoutFile{Writer: compare.OutputWriter}
	} else {
		if ci.IsView() {
			delete(intent.Options, "viewOn")
			delete(intent.Options, "pipeline")
		}
	}

	// get a document count for scheduling purposes.
	// skips this if it is a view, as it may be incredibly slow if the
	// view is based on a slow query.

	if ci.IsView() {
		return intent, nil
	}

	session, err := compare.SrcSessionProvider.GetSession()
	if err != nil {
		return nil, err
	}
	count, err := session.Database(dbName).Collection(ci.Name).EstimatedDocumentCount(context.Background())
	if err != nil {
		return nil, fmt.Errorf("error counting %v: %v", intent.Namespace(), err)
	}
	intent.Size = int64(count)
	return intent, nil
}

// CreateIntentsForDatabase iterates through collections in a db
// and builds dump intents for each collection.
func (compare *MongoCompare) CreateIntentsForDatabase(dbName string) error {
	// we must ensure folders for empty databases are still created, for legacy purposes

	session, err := compare.SrcSessionProvider.GetSession()
	if err != nil {
		return err
	}

	colsIter, err := db.GetCollections(session.Database(dbName), "")
	if err != nil {
		return fmt.Errorf("error getting collections for database `%v`: %v", dbName, err)
	}
	defer colsIter.Close(context.Background())

	for colsIter.Next(nil) {
		collInfo := &db.CollectionInfo{}
		err = colsIter.Decode(collInfo)
		if err != nil {
			return fmt.Errorf("error decoding collection info: %v", err)
		}
		if shouldSkipSystemNamespace(dbName, collInfo.Name) {
			log.Logvf(log.Info, "will not compare system collection '%s.%s'", dbName, collInfo.Name)
			continue
		}
		if compare.shouldSkipCollection(collInfo.Name) {
			log.Logvf(log.Info, "skipping compare of %v.%v, it is excluded", dbName, collInfo.Name)
			continue
		}

		if collInfo.IsView() {
			log.Logvf(log.DebugLow, "skipping compare of %v.%v because it is a view", dbName, collInfo.Name)
			continue
		}
		intent, err := compare.NewIntentFromOptions(dbName, collInfo)
		if err != nil {
			return err
		}
		compare.manager.Put(intent)
	}
	return colsIter.Err()
}

// CreateAllIntents iterates through all dbs and collections and builds
// dump intents for each collection.
func (compare *MongoCompare) CreateAllIntents() error {
	dbs, err := compare.SrcSessionProvider.DatabaseNames()
	if err != nil {
		return fmt.Errorf("error getting database names: %v", err)
	}
	log.Logvf(log.Info, "found databases: %v", strings.Join(dbs, ", "))
	for _, dbName := range dbs {
		if dbName == "local" || dbName == "admin" || dbName == "config" {
			// local can only be explicitly dumped
			log.Logvf(log.Info, "found databases: %v, pass it", dbName)
			continue
		}
		if err := compare.CreateIntentsForDatabase(dbName); err != nil {
			return fmt.Errorf("error creating intents for database %s: %v", dbName, err)
		}
	}
	return nil
}

