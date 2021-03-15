// Copyright (C) MongoDB, Inc. 2014-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package mongocompare

import (
	"fmt"
	"github.com/mongodb/mongo-tools-common/options"
)

var Usage = `<options> <connection-string>

Export the content of a running server into .bson files.

Specify a database with -d and a collection with -c to only dump that database or collection.

Connection strings must begin with mongodb:// or mongodb+srv://.

See http://docs.mongodb.com/database-tools/mongodump/ for more information.`

// InputOptions defines the set of options to use in retrieving data from the server.
type InputOptions struct {
	Query          string `long:"query" short:"q" description:"query filter, as a v2 Extended JSON string, e.g., '{\"x\":{\"$gt\":1}}'"`
	Sort           string `long:"sort" value-name:"<json>" description:"sort order, as a JSON string, e.g. '{x:1}'"`
	CheckAllDoc	   bool   `long:"checkAllDoc" default:"false" description:"check whether the content of the document is consistent, If it is not set to true, it will only check if _id exists in the target library(default false)"`
	Mode 		   string `long:"mode" value-name:"<type>" default:"dbNum" default-mask:"-" description:"the compare mode, either dbNum/tableNum/index/count/allData/someData/randomData(default dbNum)"`
	Skip           int64  `long:"skip" value-name:"<count>" description:"number of documents to skip, only be used in someData mode"`
	Limit          int64  `long:"limit" value-name:"<count>" description:"limit the number of documents query, only be used in someData and random mode"`
	ReadPreference string `long:"readPreference" value-name:"<string>|<json>" description:"specify either a preference mode (e.g. 'nearest') or a preference json object (e.g. '{mode: \"nearest\", tagSets: [{a: \"b\"}], maxStalenessSeconds: 123}')"`
	TableScan      bool   `long:"forceTableScan" description:"force a table scan (do not use $snapshot or hint _id). Deprecated since this is default behavior on WiredTiger"`
}

// Name returns a human-readable group name for input options.
func (*InputOptions) Name() string {
	return "query"
}

func (inputOptions *InputOptions) HasQuery() bool {
	return inputOptions.Query != ""
}

func (inputOptions *InputOptions) GetQuery() ([]byte, error) {
	if inputOptions.Query != "" {
		return []byte(inputOptions.Query), nil
	}
	panic("GetQuery can return valid values only for query or queryFile input")
}

// OutputOptions defines the set of options for writing dump data.
type OutputOptions struct {
	ExcludedCollections        []string `long:"excludeCollection" value-name:"<collection-name>" description:"collection to exclude from the dump (may be specified multiple times to exclude additional collections)"`
	ExcludedCollectionPrefixes []string `long:"excludeCollectionsWithPrefix" value-name:"<collection-prefix>" description:"exclude all collections from the dump that have the given prefix (may be specified multiple times to exclude additional prefixes)"`
	NumParallelCollections     int      `long:"numParallelCollections" short:"j" description:"number of collections to dump in parallel" default:"4" default-mask:"-"`
	NumComparisonWorkers        int    `long:"numComparisonWorkersPerCollection" description:"number of insert operations to run concurrently per collection" default:"1" default-mask:"-"`
}

// Name returns a human-readable group name for output options.
func (*OutputOptions) Name() string {
	return "output"
}


type SrcAuthOptions struct {
	SrcUsername        string `value-name:"<srcUsername>" long:"srcUsername" description:"src username for authentication"`
	SrcPassword        string `value-name:"<srcPassword>" long:"srcPassword" description:"src password for authentication"`
	SrcSource          string `long:"srcAuthenticationDatabase" value-name:"<src-database-name>" description:"src database that holds the user's credentials"`
	SrcMechanism       string `long:"srcAuthenticationMechanism" value-name:"<src-mechanism>" description:"authentication mechanism to use"`
}

// Name returns a human-readable group name for output options.
func (*SrcAuthOptions) Name() string {
	return "srcAuthentication"
}

type DstAuthOptions struct {
	DstUsername        string `value-name:"<dstUsername>" long:"dstUsername" description:"dst username for authentication"`
	DstPassword        string `value-name:"<dstPassword>" long:"dstPassword" description:"dst password for authentication"`
	DstSource          string `long:"dstAuthenticationDatabase" value-name:"<dst-database-name>" description:"dst database that holds the user's credentials"`
	DstMechanism       string `long:"dstAuthenticationMechanism" value-name:"<dst-mechanism>" description:"dst authentication mechanism to use"`
}

// Name returns a human-readable group name for output options.
func (*DstAuthOptions) Name() string {
	return "dstAuthentication"
}


type SrcConnectionOptions struct {
	SrcHost string `long:"srcHost" value-name:"<src-hostname>" description:"mongodb src host to connect to (setname/host1,host2 for replica sets)"`
	SrcPort string `long:"srcPort" value-name:"<src-port>" description:"server port (can also use --host hostname:port)"`

	Timeout                int    `long:"dialTimeout" default:"3" hidden:"true" description:"dial timeout in seconds"`
	SocketTimeout          int    `long:"socketTimeout" default:"0" hidden:"true" description:"socket timeout in seconds (0 for no timeout)"`
	TCPKeepAliveSeconds    int    `long:"TCPKeepAliveSeconds" default:"30" hidden:"true" description:"seconds between TCP keep alives"`
	ServerSelectionTimeout int    `long:"serverSelectionTimeout" hidden:"true" description:"seconds to wait for server selection; 0 means driver default"`
	Compressors            string `long:"compressors" default:"none" hidden:"true" value-name:"<snappy,...>" description:"comma-separated list of compressors to enable. Use 'none' to disable."`
}
func (*SrcConnectionOptions) Name() string {
	return "srcConnection"
}


type DstConnectionOptions struct {
	DstHost string `long:"dstHost" value-name:"<dst-hostname>" description:"mongodb dst host to connect to (setname/host1,host2 for replica sets)"`
	DstPort string `long:"dstPort" value-name:"<dst-port>" description:"server port (can also use --host hostname:port)"`

	Timeout                int    `long:"dialTimeout" default:"3" hidden:"true" description:"dial timeout in seconds"`
	SocketTimeout          int    `long:"socketTimeout" default:"0" hidden:"true" description:"socket timeout in seconds (0 for no timeout)"`
	TCPKeepAliveSeconds    int    `long:"TCPKeepAliveSeconds" default:"30" hidden:"true" description:"seconds between TCP keep alives"`
	ServerSelectionTimeout int    `long:"serverSelectionTimeout" hidden:"true" description:"seconds to wait for server selection; 0 means driver default"`
	Compressors            string `long:"compressors" default:"none" hidden:"true" value-name:"<snappy,...>" description:"comma-separated list of compressors to enable. Use 'none' to disable."`
}
func (*DstConnectionOptions) Name() string {
	return "dstConnection"
}


type Options struct {
	*options.ToolOptions
	*InputOptions
	*OutputOptions
	*SrcAuthOptions
	*DstAuthOptions
	*SrcConnectionOptions
	*DstConnectionOptions
}

func ParseOptions(rawArgs []string, versionStr, gitCommit string) (Options, error) {
	opts := options.New("mongocompare", versionStr, gitCommit, Usage, true, options.EnabledOptions{Auth: false, Connection: false, Namespace: true, URI: false})

	inputOpts := &InputOptions{}
	opts.AddOptions(inputOpts)
	outputOpts := &OutputOptions{}
	opts.AddOptions(outputOpts)
	opts.URI.AddKnownURIParameters(options.KnownURIOptionsReadPreference)
	srcAuthOpts := &SrcAuthOptions{}
	opts.AddOptions(srcAuthOpts)
	dstAuthOpts := &DstAuthOptions{}
	opts.AddOptions(dstAuthOpts)
	srcConnectionOpts := &SrcConnectionOptions{}
	opts.AddOptions(srcConnectionOpts)
	dstConnectionOpts := &DstConnectionOptions{}
	opts.AddOptions(dstConnectionOpts)

	extraArgs, err := opts.ParseArgs(rawArgs)
	if err != nil {
		return Options{}, err
	}

	if len(extraArgs) > 0 {
		return Options{}, fmt.Errorf("error parsing positional arguments: " +
			"provide only one MongoDB connection string. " +
			"Connection strings must begin with mongodb:// or mongodb+srv:// schemes",
		)
	}

	return Options{opts, inputOpts, outputOpts,
		srcAuthOpts, dstAuthOpts, srcConnectionOpts, dstConnectionOpts}, nil
}
