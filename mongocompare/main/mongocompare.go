
package main

import (
	"os"
	"time"

	"github.com/mongodb/mongo-tools-common/log"
	"github.com/mongodb/mongo-tools-common/progress"
	"github.com/mongodb/mongo-tools-common/signals"
	"github.com/mongodb/mongo-tools-common/util"
	"github.com/mongodb/mongo-tools/mongocompare"
)

const (
	progressBarLength   = 24
	progressBarWaitTime = time.Second * 3
)

var (
	VersionStr = "built-without-version-string"
	GitCommit  = "build-without-git-commit"
)

func main() {
	// initialize command-line opts
	opts, err := mongocompare.ParseOptions(os.Args[1:], VersionStr, GitCommit)
	if err != nil {
		log.Logvf(log.Always, "error parsing command line options: %s", err.Error())
		log.Logvf(log.Always, util.ShortUsage("mongodump"))
		os.Exit(util.ExitFailure)
	}

	// print help, if specified
	if opts.PrintHelp(false) {
		return
	}

	// print version, if specified
	if opts.PrintVersion() {
		return
	}

	// init logger
	log.SetVerbosity(opts.Verbosity)

	// verify uri options and log them
	opts.URI.LogUnsupportedOptions()

	// kick off the progress bar manager
	progressManager := progress.NewBarWriter(log.Writer(0), progressBarWaitTime, progressBarLength, false)
	progressManager.Start()
	defer progressManager.Stop()

	compare := mongocompare.MongoCompare{
		ToolOptions:     opts.ToolOptions,
		OutputOptions:   opts.OutputOptions,
		InputOptions:    opts.InputOptions,
		SrcAuthOptions: opts.SrcAuthOptions,
		DstAuthOptions: opts.DstAuthOptions,
		SrcConnection: opts.SrcConnectionOptions,
		DstConnection: opts.DstConnectionOptions,
		ProgressManager: progressManager,
	}

	finishedChan := signals.HandleWithInterrupt(compare.HandleInterrupt)
	defer close(finishedChan)

	if err = compare.Init(); err != nil {
		log.Logvf(log.Always, "Failed: %v", err)
		os.Exit(util.ExitFailure)
	}
	defer compare.SrcSessionProvider.Close()
	defer compare.DstSessionProvider.Close()

	if err = compare.Compare(); err != nil {
		log.Logvf(log.Always, "Failed: %v", err)
		os.Exit(util.ExitFailure)
	}
}
