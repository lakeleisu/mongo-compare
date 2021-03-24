// Copyright (C) MongoDB, Inc. 2014-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package mongocompare

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/mongodb/mongo-tools-common/db"
	"github.com/mongodb/mongo-tools-common/intents"
	"github.com/mongodb/mongo-tools-common/log"
	"github.com/mongodb/mongo-tools-common/options"
	"github.com/mongodb/mongo-tools-common/progress"
	"github.com/mongodb/mongo-tools-common/util"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	mongoOpt "go.mongodb.org/mongo-driver/mongo/options"
	"bufio"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

type storageEngineType int

const (
	storageEngineUnknown = 0
	storageEngineMMAPV1  = 1
	storageEngineModern  = 2
)

const defaultPermissions = 0755

// MongoDump is a container for the user-specified options and
// internal state used for running mongodump.
type MongoCompare struct {
	// basic mongo tool options
	ToolOptions   *options.ToolOptions
	InputOptions  *InputOptions
	OutputOptions *OutputOptions
	SrcAuthOptions *SrcAuthOptions
	SrcConnection *SrcConnectionOptions
	DstAuthOptions *DstAuthOptions
	DstConnection *DstConnectionOptions
	ProgressManager progress.Manager

	// useful internals that we don't directly expose as options
	SrcSessionProvider *db.SessionProvider
	DstSessionProvider *db.SessionProvider
	manager         *intents.Manager
	query           bson.D
	isMongos        bool
	storageEngine   storageEngineType
	authVersion     int
	// shutdownIntentsNotifier is provided to the multiplexer
	// as well as the signal handler, and allows them to notify
	// the intent dumpers that they should shutdown
	shutdownIntentsNotifier *notifier
	// Writer to take care of BSON output when not writing to the local filesystem.
	// This is initialized to os.Stdout if unset.
	OutputWriter io.Writer

	// XXX Unused?!?
	// readPrefMode mgo.Mode
	// readPrefTags []bson.D
}

type notifier struct {
	notified chan struct{}
	once     sync.Once
}

func (n *notifier) Notify() { n.once.Do(func() { close(n.notified) }) }

func newNotifier() *notifier { return &notifier{notified: make(chan struct{})} }

// ValidateOptions checks for any incompatible sets of options.
func (compare *MongoCompare) ValidateOptions() error {
	switch {
	case compare.OutputOptions.Out == "-" && compare.ToolOptions.Namespace.Collection == "":
		return fmt.Errorf("can only compare a single collection to stdout")
	case compare.ToolOptions.Namespace.DB == "" && compare.ToolOptions.Namespace.Collection != "":
		return fmt.Errorf("cannot compare a collection without a specified database")
	case compare.InputOptions.Query != "" && compare.ToolOptions.Namespace.Collection == "":
		return fmt.Errorf("cannot compare using a query without a specified collection")

	case compare.InputOptions.Query != "" && compare.InputOptions.TableScan:
		return fmt.Errorf("cannot use --forceTableScan when specifying --query")
	case len(compare.OutputOptions.ExcludedCollections) > 0 && compare.ToolOptions.Namespace.Collection != "":
		return fmt.Errorf("--collection is not allowed when --excludeCollection is specified")
	case len(compare.OutputOptions.ExcludedCollectionPrefixes) > 0 && compare.ToolOptions.Namespace.Collection != "":
		return fmt.Errorf("--collection is not allowed when --excludeCollectionsWithPrefix is specified")
	case len(compare.OutputOptions.ExcludedCollections) > 0 && compare.ToolOptions.Namespace.DB == "":
		return fmt.Errorf("--db is required when --excludeCollection is specified")
	case len(compare.OutputOptions.ExcludedCollectionPrefixes) > 0 && compare.ToolOptions.Namespace.DB == "":
		return fmt.Errorf("--db is required when --excludeCollectionsWithPrefix is specified")
	case compare.OutputOptions.NumParallelCollections <= 0:
		return fmt.Errorf("numParallelCollections must be positive")
	case compare.OutputOptions.NumComparisonWorkers <= 0:
		return fmt.Errorf("numComparisonWorkersPerCollection must be positive")
	case compare.InputOptions.Mode == "randomData" && compare.InputOptions.Limit == 0:
		return fmt.Errorf("--limit is required when randomData mode is specified")
	case compare.InputOptions.Mode == "someData" && compare.InputOptions.Limit == 0:
		return fmt.Errorf("--limit is required when someData mode is specified")

	}
	return nil
}

// Init performs preliminary setup operations for MongoDump.
func (compare *MongoCompare) Init() error {
	log.Logvf(log.DebugHigh, "initializing mongocompare object")

	// this would be default, but explicit setting protects us from any
	// redefinition of the constants.
	compare.storageEngine = storageEngineUnknown

	err := compare.ValidateOptions()
	if err != nil {
		return fmt.Errorf("bad option: %v", err)
	}
	if compare.OutputWriter == nil {
		compare.OutputWriter = os.Stdout
	}

	pref, err := db.NewReadPreference(compare.InputOptions.ReadPreference, nil)
	if err != nil {
		return fmt.Errorf("error parsing --readPreference : %v", err)
	}
	compare.ToolOptions.ReadPreference = pref


	srcToolOptions := options.ToolOptions{
		Auth :&options.Auth{
			Username: compare.SrcAuthOptions.SrcUsername,
			Password:  compare.SrcAuthOptions.SrcPassword,
			Source : compare.SrcAuthOptions.SrcSource,
			Mechanism: compare.SrcAuthOptions.SrcMechanism,
		},
		Connection :&options.Connection{
			Host: compare.SrcConnection.SrcHost,
			Port: compare.SrcConnection.SrcPort,
		},
		General:    &options.General{},
		Verbosity:  &options.Verbosity{},
		URI:        &options.URI{},
		SSL:        &options.SSL{},
		Namespace:  &options.Namespace{},
		Kerberos:   &options.Kerberos{},
	}

	srcToolOptions.NormalizeOptionsAndURI()
	srcToolOptions.UseSSL = false
	srcToolOptions.URI.LogUnsupportedOptions()
	compare.SrcSessionProvider, err = db.NewSessionProvider(srcToolOptions)
	if err != nil {
		return fmt.Errorf("can't create session: %v", err)
	}

	dstToolOptions := options.ToolOptions{
		Auth :&options.Auth{
			Username: compare.DstAuthOptions.DstUsername,
			Password:  compare.DstAuthOptions.DstPassword,
			Source : compare.DstAuthOptions.DstSource,
			Mechanism: compare.DstAuthOptions.DstMechanism,
		},
		Connection :&options.Connection{
			Host: compare.DstConnection.DstHost,
			Port: compare.DstConnection.DstPort,
		},
		General:    &options.General{},
		Verbosity:  &options.Verbosity{},
		URI:        &options.URI{},
		SSL:        &options.SSL{},
		Namespace:  &options.Namespace{},
		Kerberos:   &options.Kerberos{},
	}
	dstToolOptions.NormalizeOptionsAndURI()
	dstToolOptions.UseSSL = false
	dstToolOptions.URI.LogUnsupportedOptions()
	compare.DstSessionProvider, err = db.NewSessionProvider(dstToolOptions)
	if err != nil {
		return fmt.Errorf("can't create session: %v", err)
	}

	// warn if we are trying to dump from a secondary in a sharded cluster
	if compare.isMongos && pref != readpref.Primary() {
		log.Logvf(log.Always, db.WarningNonPrimaryMongosConnection)
	}

	compare.manager = intents.NewIntentManager()

	return nil
}

func (compare *MongoCompare) verifyCollectionExists() (bool, error) {
	// Running MongoDump against a DB with no collection specified works. In this case, return true so the process
	// can continue.
	if compare.ToolOptions.Namespace.Collection == "" {
		return true, nil
	}

	coll := compare.SrcSessionProvider.DB(compare.ToolOptions.Namespace.DB).Collection(compare.ToolOptions.Namespace.Collection)
	collInfo, err := db.GetCollectionInfo(coll)
	if err != nil {
		return false, err
	}

	return collInfo != nil, nil
}

// Dump handles some final options checking and executes MongoDump.
func (compare *MongoCompare) Compare() (err error) {

	exists, err := compare.verifyCollectionExists()
	if err != nil {
		return fmt.Errorf("error verifying collection info: %v", err)
	}
	if !exists {
		log.Logvf(log.Always, "namespace with DB %s and collection %s does not exist",
			compare.ToolOptions.Namespace.DB, compare.ToolOptions.Namespace.Collection)
		return nil
	}

	log.Logvf(log.Always, "starting compare")

	compare.shutdownIntentsNotifier = newNotifier()

	if compare.InputOptions.HasQuery() {
		content, err := compare.InputOptions.GetQuery()
		if err != nil {
			return err
		}
		var query bson.D
		err = bson.UnmarshalExtJSON(content, false, &query)
		if err != nil {
			return fmt.Errorf("error parsing query as Extended JSON: %v", err)
		}
		compare.query = query
	}

	// Confirm connectivity
	srcSession, err := compare.SrcSessionProvider.GetSession()
	if err != nil {
		return fmt.Errorf("error getting a src client session: %v", err)
	}
	err = srcSession.Ping(context.Background(), nil)
	if err != nil {
		return fmt.Errorf("error connecting to src host: %v", err)
	}

	dstSession, err := compare.DstSessionProvider.GetSession()
	if err != nil {
		return fmt.Errorf("error getting a dst client session: %v", err)
	}
	err = dstSession.Ping(context.Background(), nil)
	if err != nil {
		return fmt.Errorf("error connecting to dst host: %v", err)
	}
	log.Logvf(log.Always, "enter compare mode %v", compare.InputOptions.Mode)
	switch compare.InputOptions.Mode {
	case "dbNum":
		return 	compare.compareDbNum()
	case "tableNum":
		return 	compare.compareTableNum()
	case "count":
		return 	compare.compareCount()
	case "allData":
		return 	compare.compareIntent()
	case "someData":
		return 	compare.compareIntent()
	case "randomData":
		return 	compare.compareIntent()
	case "index":
		return 	compare.compareTableIndex()

		
	}

	return nil

}

func (compare *MongoCompare) compareDbNum()(err error) {
	srcDbName, err := compare.SrcSessionProvider.DatabaseNames()
	if err != nil {
		log.Logvf(log.Always, "get src database name fail, err: %v", err)
		return err
	}

	dstDbName, err := compare.DstSessionProvider.DatabaseNames()
	if err != nil {
		log.Logvf(log.Always, "get dst database name fail, err: %v", err)
		return err
	}

	if len(srcDbName) != len(dstDbName) {
		log.Logvf(log.Always, "Fail! database name not match, src:%v, dst:%v", len(srcDbName), len(dstDbName))
	}


	dstDbNameDict := make(map[string]bool)
	for _, dbName := range dstDbName{
		dstDbNameDict[dbName] = true
	}

	hasDiff := false
	for _, dbName := range srcDbName{
		if dbName == "local" || dbName == "admin" || dbName == "config"  {
			continue
		}
		_, exist := dstDbNameDict[dbName]
		if !exist {
			hasDiff = true
			log.Logvf(log.Always, "Fail! src database name :%v not exist in dst", dbName)
		}
	}

	if !hasDiff {
		log.Logvf(log.Always, "OK!   compare finish, all database ok.")
	} else {
		log.Logvf(log.Always, "compare finish.")
	}
    return nil
}

func (compare *MongoCompare) compareTableNum()(err error) {
	srcDbNames := make([]string, 0)
	switch {
	case compare.ToolOptions.DB == "":
		srcDbNames, err = compare.SrcSessionProvider.DatabaseNames()
		if err != nil {
			log.Logvf(log.Always, "get src database name fail, err: %v", err)
			return err
		}
	case compare.ToolOptions.DB != "":
		srcDbNames = append(srcDbNames, compare.ToolOptions.DB)

	}

	hasDiff := false
	for _, srcDb := range srcDbNames {
		if srcDb == "local" || srcDb == "admin" || srcDb == "config"  {
			continue
		}
		srcTables, err := compare.SrcSessionProvider.DB(srcDb).ListCollectionNames(context.Background(), bson.M{})
		if err != nil {
			log.Logvf(log.Always, "get src table names fail, err: %v", err)
			return err
		}

		dstTables, err := compare.DstSessionProvider.DB(srcDb).ListCollectionNames(context.Background(), bson.M{})
		if err != nil {
			log.Logvf(log.Always, "get src table names fail, err: %v", err)
			return err
		}
		// 表个数不一致
		if len(srcTables) != len(dstTables) {
			hasDiff = true
			log.Logvf(log.Always, "Fail! database name:%v table number not match, src:%v, dst:%v", srcDb, len(srcTables), len(dstTables))
		}
		dstTableNameDict := make(map[string]bool)
		for _, dstTable := range dstTables{
			dstTableNameDict[dstTable] = true
		}
		//源端表不在目标端
		for _, srcTable := range srcTables {
			_, exit := dstTableNameDict[srcTable]
			if !exit {
				hasDiff = true
				log.Logvf(log.Always, "Fail! database name:%v table:%v not in dst", srcDb, srcTable)
			}
		}
	}

	if !hasDiff {
		log.Logvf(log.Always, "OK!   compare finish, all database ok.")
	} else {
		log.Logvf(log.Always, "compare finish.")
	}

	return nil
}


func (compare *MongoCompare) compareTableIndex()(err error) {
	srcDbNames := make([]string, 0)
	switch {
	case compare.ToolOptions.DB == "":
		srcDbNames, err = compare.SrcSessionProvider.DatabaseNames()
		if err != nil {
			log.Logvf(log.Always, "get src database name fail, err: %v", err)
			return err
		}
	case compare.ToolOptions.DB != "":
		srcDbNames = append(srcDbNames, compare.ToolOptions.DB)

	}

	hasDiff := false
	for _, srcDb := range srcDbNames {
		if srcDb == "local" || srcDb == "admin" || srcDb == "config"  {
			continue
		}
		srcTables, err := compare.SrcSessionProvider.DB(srcDb).ListCollectionNames(context.Background(), bson.M{})
		if err != nil {
			log.Logvf(log.Always, "get src table names fail, err: %v", err)
			return err
		}

		dstTables, err := compare.DstSessionProvider.DB(srcDb).ListCollectionNames(context.Background(), bson.M{})
		if err != nil {
			log.Logvf(log.Always, "dst table names fail, err: %v", err)
			return err
		}
		//源端表不在目标端
		dstTableDict := make(map[string]bool)
		for _, dstTable := range dstTables {
			dstTableDict[dstTable] = true
		}


		//源端表不在目标端
		for _, srcTable := range srcTables {
			_, exist := dstTableDict[srcTable]
			if !exist {
				log.Logvf(log.Always, "dst table %v.%v tables not exist, pass check index.", srcDb, srcTable)
				continue
			}

			srcIndexes, err := compare.SrcSessionProvider.DB(srcDb).Collection(srcTable).Indexes().List(context.Background())
			if err != nil {
				log.Logvf(log.Always, "src table %v.%v index fail, err: %v", srcDb, srcTable, err)
				return err
			}

			dstIndexes, err := compare.DstSessionProvider.DB(srcDb).Collection(srcTable).Indexes().List(context.Background())
			if err != nil {
				log.Logvf(log.Always, "get src table %v.%v index fail, err: %v", srcDb, srcTable, err)
				return err
			}
			type indexRes struct {
				Key bson.D
				Name string
			}

			dstIndexDict := make(map[string]bool)
			for dstIndexes.Next(context.Background()) {
				var res indexRes
				err = dstIndexes.Decode(&res)
				if err != nil {
					log.Logvf(log.Always, "decode src table %v.%v index fail, err: %v", srcDb, srcTable, err)
					return err
				}

				json, _ := bson.MarshalExtJSON(res.Key, true, false)
				if err != nil {
					log.Logvf(log.Always, "marshal src table %v.%v index fail, err: %v", srcDb, srcTable, err)
					return err
				}
				dstIndexDict[string(json)] = true
			}

			for srcIndexes.Next(context.Background()) {
				var res indexRes
				err = srcIndexes.Decode(&res)
				if err != nil {
					log.Logvf(log.Always, "decode src table %v.%v index fail, err: %v", srcDb, srcTable, err)
					return err
				}

				json, _ := bson.MarshalExtJSON(res.Key, true, false)
				_, exist := dstIndexDict[string(json)]
				if !exist {
					hasDiff = true
					log.Logvf(log.Always, "result: table %v.%v , index name: %v, index key: %v not exist in dst.", srcDb, srcTable, res.Name, string(json))
				} else {
					log.Logvf(log.DebugLow, "result: table %v.%v index name: %v, index key: %v  exist in dst.", srcDb, srcTable, res.Name, string(json))
				}
			}

		}
	}

	if !hasDiff {
		log.Logvf(log.Always, "OK!   compare finish, all database ok.")
	} else {
		log.Logvf(log.Always, "compare finish.")
	}

	return nil
}

func (compare *MongoCompare) compareCount()(err error) {
	srcDbNames := make([]string, 0)
	switch {
	case compare.ToolOptions.DB == "":
		srcDbNames, err = compare.SrcSessionProvider.DatabaseNames()
		if err != nil {
			log.Logvf(log.Always, "get src database name fail, err: %v", err)
			return err
		}

	case compare.ToolOptions.DB != "":
		srcDbNames = append(srcDbNames, compare.ToolOptions.DB)

	}
	for _, srcDb := range srcDbNames {
		if srcDb == "local" || srcDb == "admin" || srcDb == "config"  {
			continue
		}

		srcTables, err := compare.SrcSessionProvider.DB(srcDb).ListCollectionNames(context.Background(), bson.M{})
		if err != nil {
			log.Logvf(log.Always, "get src table names fail, err: %v", err)
			return err
		}

		//源端表不在目标端
		for _, srcTable := range srcTables {
			if len(compare.ToolOptions.Collection) != 0 && srcTable != compare.ToolOptions.Collection {
				log.Logvf(log.DebugLow, "table: %v pass.", srcTable)
				continue
			}
			srcCount, err := compare.SrcSessionProvider.DB(srcDb).Collection(srcTable, &mongoOpt.CollectionOptions{ReadPreference: compare.ToolOptions.ReadPreference}).CountDocuments(context.Background(), bson.M{})
			if err != nil {
				log.Logvf(log.Always, "get src table count fail, err: %v", err)
				return err
			}

			dstCount, err := compare.DstSessionProvider.DB(srcDb).Collection(srcTable, &mongoOpt.CollectionOptions{ReadPreference: compare.ToolOptions.ReadPreference}).CountDocuments(context.Background(), bson.M{})
			if err != nil {
				log.Logvf(log.Always, "dst table count fail, err: %v", err)
				return err
			}
			// 表个数不一致
			if srcCount != dstCount {
				log.Logvf(log.Always, "Fail! database name:%v table:%v count not match, src:%v, dst:%v", srcDb, srcTable, srcCount, dstCount)
			} else {
				log.Logvf(log.Always, "OK!   database name:%v table:%v count:%v match", srcDb, srcTable, srcCount)
			}

		}
	}
	log.Logvf(log.Always, "compare finish.")
	return nil
}

func (compare *MongoCompare) compareIntent()(err error) {
	switch {
	case compare.ToolOptions.DB == "" && compare.ToolOptions.Collection == "":
		err = compare.CreateAllIntents()
	case compare.ToolOptions.DB != "" && compare.ToolOptions.Collection == "":
		err = compare.CreateIntentsForDatabase(compare.ToolOptions.DB)
	case compare.ToolOptions.DB != "" && compare.ToolOptions.Collection != "":
		err = compare.CreateCollectionIntent(compare.ToolOptions.DB, compare.ToolOptions.Collection)
	}
	if err != nil {
		return fmt.Errorf("error creating intents to dump: %v", err)
	}

	log.Logvf(log.Always, "begin compare")

	if err := compare.CompareIntents(); err != nil {
		return err
	}

	log.Logvf(log.Always, "finishing compare")

	return err
}


type resettableOutputBuffer interface {
	io.Writer
	Close() error
	Reset(io.Writer)
}

type closableBufioWriter struct {
	*bufio.Writer
}

func (w closableBufioWriter) Close() error {
	return w.Flush()
}

func (compare *MongoCompare) getResettableOutputBuffer() resettableOutputBuffer {
	return &closableBufioWriter{bufio.NewWriter(nil)}
}

// DumpIntents iterates through the previously-created intents and
// dumps all of the found collections.
func (compare *MongoCompare) CompareIntents() error {
	resultChan := make(chan error)

	jobs := compare.OutputOptions.NumParallelCollections
	if numIntents := len(compare.manager.Intents()); jobs > numIntents {
		jobs = numIntents
	}

	if jobs > 1 {
		compare.manager.Finalize(intents.LongestTaskFirst)
	} else {
		compare.manager.Finalize(intents.Legacy)
	}

	log.Logvf(log.Info, "comparing up to %v collections in parallel", jobs)

	// start a goroutine for each job thread
	for i := 0; i < jobs; i++ {
		go func(id int) {

			log.Logvf(log.DebugHigh, "starting compare routine with id=%v", id)
			for {
				intent := compare.manager.Pop()
				if intent == nil {
					log.Logvf(log.DebugHigh, "ending compare routine with id=%v, no more work to do", id)
					resultChan <- nil
					return
				}
				log.Logvf(log.Info, "start compare %v.%v", intent.DB, intent.C)

				if intent.DB == "local" || intent.DB == "admin" || intent.DB == "config"  {
					log.Logvf(log.Info, "%v pass.", intent.DB)
					resultChan <- nil
					return
				}

				dstTables, err := compare.DstSessionProvider.DB(intent.DB).ListCollectionNames(context.Background(), bson.M{})
				if err != nil {
					log.Logvf(log.Always, "get src table names fail, err: %v", err)
					resultChan <- err
					return
				}
				//源端表不在目标端
				exist := false
				for _, dstTable := range dstTables {
					if dstTable == intent.C {
						exist = true
					}
				}
				if !exist {
					log.Logvf(log.Info, "result: %v.%v not exist in dst, no need check document.", intent.DB, intent.C)
					resultChan <- nil
					return
				}

				err = compare.CompareIntent(intent)
				if err != nil {
					resultChan <- err
					return
				}

				compare.manager.Finish(intent)
			}
		}(i)
	}

	// wait until all goroutines are done or one of them errors out
	for i := 0; i < jobs; i++ {
		if err := <-resultChan; err != nil {
			return err
		}
	}

	return nil
}

// DumpIntent dumps the specified database's collection.
func (compare *MongoCompare) CompareIntent(intent *intents.Intent) error {
	session, err := compare.SrcSessionProvider.GetSession()
	if err != nil {
		return err
	}
	intendedDB := session.Database(intent.DB)
	coll := intendedDB.Collection(intent.C)
	// it is safer to assume that a collection is a view, if we cannot determine that it is not.
	isView := true
	// failure to get CollectionInfo should not cause the function to exit. We only use this to
	// determine if a collection is a view.
	collInfo, err := db.GetCollectionInfo(coll)
	if err != nil {
		return err
	} else if collInfo != nil {
		isView = collInfo.IsView()
	}
	// The storage engine cannot change from namespace to namespace,
	// so we set it the first time we reach here, using a namespace we
	// know must exist. If the storage engine is not mmapv1, we assume it
	// is some modern storage engine that does not need to use an index
	// scan for correctness.
	// We cannot determine the storage engine, if this collection is a view,
	// so we skip attempting to deduce the storage engine.
	if compare.storageEngine == storageEngineUnknown && !isView {
		if err != nil {
			return err
		}
		// storageEngineModern denotes any storage engine that is not MMAPV1. For such storage
		// engines we assume that collection scans are consistent.
		compare.storageEngine = storageEngineModern
		isMMAPV1, err := db.IsMMAPV1(intendedDB, intent.C)
		if err != nil {
			log.Logvf(log.Always,
				"failed to determine storage engine, an mmapv1 storage engine could result in"+
					" inconsistent dump results, error was: %v", err)
		} else if isMMAPV1 {
			compare.storageEngine = storageEngineMMAPV1
		}
	}

	findQuery := &db.DeferredQuery{Coll: coll}
	switch {
	case len(compare.query) > 0:
		findQuery.Filter = compare.query
	// we only want to hint _id when the storage engine is MMAPV1 and this isn't a view, a
	// special collection, the oplog, and the user is not asking to force table scans.
	case compare.storageEngine == storageEngineMMAPV1 && !compare.InputOptions.TableScan &&
		!isView && !intent.IsSpecialCollection() && !intent.IsOplog():
		autoIndexId, found := intent.Options["autoIndexId"]
		if !found || autoIndexId == true {
			findQuery.Hint = bson.D{{"_id", 1}}
		}
	}

	var compareCount int64

	log.Logvf(log.Always, "writing %v to stdout", intent.Namespace())
	compareCount, err = compare.compareQueryToIntent(intent)
	if err != nil {
		// on success, print the document count
		log.Logvf(log.Always, "compared fail, err:%v", err)
		return err
	}

	log.Logvf(log.Always, "done compare %v (%v %v)", intent.Namespace(), compareCount, docPlural(compareCount))
	return nil
}

// documentValidator represents a callback used to validate individual documents. It takes a slice of bytes for a
// BSON document and returns a non-nil error if the document is not valid.
type documentValidator func([]byte) error

// dumpQueryToIntent takes an mgo Query, its intent, and a writer, performs the query,
// and writes the raw bson results to the writer. Returns a final count of documents
// dumped, and any errors that occurred.
func (compare *MongoCompare) compareQueryToIntent(intent *intents.Intent) (dumpCount int64, err error) {
	return compare.compareValidatedQueryToIntent(intent, nil)
}

// getCount counts the number of documents in the namespace for the given intent. It does not run the count for
// the oplog collection to avoid the performance issue in TOOLS-2068.
func (compare *MongoCompare) getCount(intent *intents.Intent) (int64, error) {
	session, err := compare.SrcSessionProvider.GetSession()
	if err != nil {
		return 0, err
	}

	intendedCollection := session.Database(intent.DB).Collection(intent.C)

	var total int64 = 0

	if len(compare.query) != 0 || intent.IsOplog() {
		query := bson.D{}
		if compare.InputOptions != nil && compare.InputOptions.HasQuery() {
			var err error
			content, err := compare.InputOptions.GetQuery()
			if err != nil {
				return 0, err
			}
			err = bson.UnmarshalExtJSON(content, false, &query)
			if err != nil {
				return 0, fmt.Errorf("error parsing query as Extended JSON: %v", err)
			}
		}

		total, err = intendedCollection.CountDocuments(nil, query)
		if err != nil {
			return 0, fmt.Errorf("error getting count from db: %v", err)
		}
		return 0, nil
	} else {
		total, err = intendedCollection.EstimatedDocumentCount(nil)
		if err != nil {
			return 0, fmt.Errorf("error getting estimated count from db: %v", err)
		}

	}

	log.Logvf(log.DebugLow, "counted %v %v in %v", total, docPlural(int64(total)), intent.Namespace())
	return int64(total), nil
}

// dumpValidatedQueryToIntent takes an mgo Query, its intent, a writer, and a document validator, performs the query,
// validates the results with the validator,
// and writes the raw bson results to the writer. Returns a final count of documents
// dumped, and any errors that occurred.
func (compare *MongoCompare) compareValidatedQueryToIntent(intent *intents.Intent, validator documentValidator) (compareCount int64, err error) {

	// don't dump any data for views being dumped as views
	if intent.IsView() {
		return 0, nil
	}

	total := compare.InputOptions.Limit
	cnt, err := compare.getCount(intent)
	if err != nil {
		return 0, err
	}
	if compare.InputOptions.Limit == 0 || compare.InputOptions.Limit > cnt {
		total = cnt
	}

	compareProgressor := progress.NewCounter(total)
	if compare.ProgressManager != nil {
		compare.ProgressManager.Attach(intent.Namespace(), compareProgressor)
		defer compare.ProgressManager.Detach(intent.Namespace())
	}

	err = compare.compareData(intent.DB, intent.C, compareProgressor, validator)
	if err != nil {
		log.Logvf(log.DebugHigh, "%v compare data fail, err:%v ", intent.Namespace(), err)
	}
	compareCount, _ = compareProgressor.Progress()
	if err != nil {
		err = fmt.Errorf("error writing data for collection `%v` to disk: %v", intent.Namespace(), err)
	}
	return
}

// getSortFromArg takes a sort specification in JSON and returns it as a bson.D
// object which preserves the ordering of the keys as they appear in the input.
func getSortFromArg(queryRaw string) (bson.D, error) {
	parsedJSON := bson.D{}
	err := json.Unmarshal([]byte(queryRaw), &parsedJSON)
	if err != nil {
		return nil, fmt.Errorf("query '%v' is not valid JSON: %v", queryRaw, err)
	}
	// TODO: verify sort specification before returning a nil error
	return parsedJSON, nil
}

func (compare *MongoCompare) getCompareCursor(dbName, collectionName string) (*mongo.Cursor, error) {
	findOpts := mongoOpt.Find()

	if compare.InputOptions != nil && compare.InputOptions.Sort != "" {
		sortD, err := getSortFromArg(compare.InputOptions.Sort)
		if err != nil {
			return nil, err
		}

		findOpts.SetSort(sortD)
	}

	query := bson.D{}
	if compare.InputOptions != nil && compare.InputOptions.HasQuery() {
		var err error
		content, err := compare.InputOptions.GetQuery()
		if err != nil {
			return nil, err
		}
		err = bson.UnmarshalExtJSON(content, false, &query)
		if err != nil {
			return nil, fmt.Errorf("error parsing query as Extended JSON: %v", err)
		}
	}

	session, err := compare.SrcSessionProvider.GetSession()
	if err != nil {
		return nil, err
	}
	intendedDB := session.Database(dbName)

	collection := intendedDB.Collection(collectionName)
	collectionInfo, err := db.GetCollectionInfo(collection)
	if err != nil {
		log.Logvf(log.Always,
			"failed to get collection info, error was: %v", err)
		return nil, err
	}

	isMMAPV1, err := db.IsMMAPV1(intendedDB, collectionName)
	if err != nil {
		// if we failed to determine storage engine, there is a good change it is because this
		// collection is a view. We only want to warn if this collection is not a view, since
		// storage engine does not affect consistency for scans of views.
		if !collectionInfo.IsView() {
			log.Logvf(log.Always,
				"failed to determine storage engine, an mmapv1 storage engine could"+
					" result in inconsistent export results, error was: %v", err)
		}
	}
	// shouldHintId is true iff the storage engine is MMAPV1 and the user did not specify
	// --forceTableScan.
	shouldHintId := isMMAPV1 && (compare.InputOptions == nil || !compare.InputOptions.TableScan)
	// noSorting is true if the user did not ask for sorting.
	noSorting := compare.InputOptions == nil || compare.InputOptions.Sort == ""
	coll := intendedDB.Collection(collectionName)

	// we want to hint _id if shouldHintId is true, and there is no query, and
	// there is no sorting, as hinting is not needed if there is a query or sorting.
	// we also do not want to hint for system collections or views.
	if shouldHintId && len(query) == 0 && noSorting &&
		!collectionInfo.IsView() && !collectionInfo.IsSystemCollection() {

		// Don't hint autoIndexId:false collections
		autoIndexId, found := collectionInfo.Options["autoIndexId"]
		if !found || autoIndexId == true {
			findOpts.SetHint(bson.D{{"_id", 1}})
		}
	}


	if compare.InputOptions.Mode == "randomData" {
		pipeline := mongo.Pipeline{
			{
				{"$sample", bson.D{
					{"size", compare.InputOptions.Limit},
				}},
			},

		}
		return coll.Aggregate(nil, pipeline)
	} else {
		if compare.InputOptions != nil && compare.InputOptions.Skip != 0 {
			findOpts.SetSkip(compare.InputOptions.Skip)
		}
		if compare.InputOptions != nil && compare.InputOptions.Limit != 0 {
			findOpts.SetLimit(compare.InputOptions.Limit)
		}
		return coll.Find(nil, query, findOpts)
	}
}


type doc struct {
	ID    interface{} `bson:"_id"`
}
// dumpValidatedIterToWriter takes a cursor, a writer, an Updateable object, and a documentValidator and validates and
// dumps the iterator's contents to the writer.
func (compare *MongoCompare) compareData(dbName, tableName string, progressCount progress.Updateable, validator documentValidator) error {

	var termErr error

	// We run the result iteration in its own goroutine,
	// this allows disk i/o to not block reads from the db,
	// which gives a slight speedup on benchmarks
	buffChan := make(chan doc)
	resultChan := make(chan Result, compare.OutputOptions.NumComparisonWorkers)
	go func()() {
		ctx := context.Background()
		cursor, err := compare.getCompareCursor(dbName, tableName)
		if err != nil {
			return
		}
		defer cursor.Close(context.Background())
		for {
			select {
			case <-compare.shutdownIntentsNotifier.notified:
				log.Logvf(log.DebugHigh, "terminating writes")
				termErr = util.ErrTerminated
				close(buffChan)
				return
			default:
				if !cursor.Next(ctx) {
					if err := cursor.Err(); err != nil {
						termErr = err
					}
					close(buffChan)
					return
				}

				if validator != nil {
					if err := validator(cursor.Current); err != nil {
						termErr = err
						close(buffChan)
						return
					}
				}
				doc := doc{}
				if cursorErr := cursor.Decode(&doc); cursorErr != nil {
					log.Logvf(log.Always, "decode cursor fail, err:%v", cursorErr)
					return
				}

				buffChan <- doc
			}
		}
	}()

	for i := 0; i < compare.OutputOptions.NumComparisonWorkers; i++ {

		go func() {
			var result Result
			session, err := compare.DstSessionProvider.GetSession()
			if err != nil {
				result.Err = err
				resultChan <- result
				return
			}
			for doc := range buffChan {
				log.Logvf(log.DebugHigh, "checking doc:%v exist in %v.%v", doc.ID, dbName, tableName)
				filter := bson.D{{"_id", doc.ID}}
				maxTime := time.Duration(30) * time.Second
				opt := mongoOpt.FindOneOptions{MaxTime: &maxTime}

				findResult := session.Database(dbName).Collection(tableName).FindOne(context.Background(), filter, &opt)
				if findResult == nil {
					result.Err = errors.New("return nil")
					resultChan <- result
					return
				}

				nSuccess := 0
				nFailure := 0
				if findResult.Err() == mongo.ErrNoDocuments {
					nFailure = 1
					log.Logvf(log.Always, "result: %v not exist in dst database %v.%v", doc.ID, dbName, tableName)
				} else if findResult.Err() == nil {
					log.Logvf(log.DebugLow, "result: %v exist in dst database %v.%v", doc.ID, dbName, tableName)
					nSuccess = 1
				} else {
					log.Logvf(log.Always, "err:%v", result.Err)
					result.Err = findResult.Err()
				}

				progressCount.Inc(1)
				result.combineWith(Result{int64(nSuccess), int64(nFailure), result.Err})
				if result.Err != nil {
					log.Logvf(log.Always, "find doc:%v exist in dst fail, err:%v, exit.", doc.ID, findResult.Err())
					resultChan <- result
					return
				}
			}
			result.Err = nil
			resultChan <- result
			return
		}()

	}
	// wait until all goroutines are done or one of them errors out
	for i := 0; i < compare.OutputOptions.NumComparisonWorkers; i++ {
		err := <-resultChan
		if err.Err != nil {
			log.Logvf(log.Always, "compare index %v fail, err:%v.", i, err.Err)
			close(compare.shutdownIntentsNotifier.notified)
		}
	}

	return termErr
}


// docPlural returns "document" or "documents" depending on the
// count of documents passed in.
func docPlural(count int64) string {
	return util.Pluralize(int(count), "document", "documents")
}

func (compare *MongoCompare) HandleInterrupt() {
	if compare.shutdownIntentsNotifier != nil {
		compare.shutdownIntentsNotifier.Notify()
	}
}


type Result struct {
	Successes int64
	Failures  int64
	Err       error
}

// log pretty-prints the result, associated with restoring the given namespace
func (result *Result) log(ns string) {
	log.Logvf(log.Always, "finished restoring %v (%v %v, %v %v)",
		ns, result.Successes, util.Pluralize(int(result.Successes), "document", "documents"),
		result.Failures, util.Pluralize(int(result.Failures), "failure", "failures"))
}

// combineWith sums the successes and failures from both results and the overwrites the existing Err with the Err from
// the provided result.
func (result *Result) combineWith(other Result) {
	result.Successes += other.Successes
	result.Failures += other.Failures
	result.Err = other.Err
}

// withErr returns a copy of the current result with the provided error
func (result Result) withErr(err error) Result {
	result.Err = err
	return result
}
