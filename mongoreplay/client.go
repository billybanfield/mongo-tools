package mongoreplay

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime/pprof"
	"time"
)

// ClientCommand stores settings for the mongoreplay 'client' subcommand
type ClientCommand struct {
	GlobalOpts   *Options `no-flag:"true"`
	PlaybackFile string   `description:"path to the playback file to play from" short:"p" long:"playback-file" required:"yes"`
	Speed        float64  `description:"multiplier for playback speed (1.0 = real-time, .5 = half-speed, 3.0 = triple-speed, etc.)" long:"speed" default:"1.0"`
	ServerURL    string   `short:"s" long:"server" description:"Location of the server to send data to" default:"mongodb://localhost:27017"`
	Repeat       int      `long:"repeat" description:"Number of times to play the playback file" default:"1"`
	QueueTime    int      `long:"queueTime" description:"don't queue ops much further in the future than this number of seconds" default:"15"`
	NoPreprocess bool     `long:"no-preprocess" description:"don't preprocess the input file to premap data such as mongo cursorIDs"`
	Gzip         bool     `long:"gzip" description:"decompress gzipped input"`
	Collect      string   `long:"collect" description:"Stat collection format; 'format' option uses the --format string" choice:"json" choice:"format" choice:"none" default:"none"`
	FullSpeed    bool     `long:"fullSpeed" description:"run the playback as fast as possible"`
}

// ValidateParams validates the settings described in the PlayCommand struct.
func (client *ClientCommand) ValidateParams(args []string) error {
	switch {
	case len(args) > 0:
		return fmt.Errorf("unknown argument: %s", args[0])
	case client.Speed <= 0:
		return fmt.Errorf("Invalid setting for --speed: '%v'", client.Speed)
	case client.Repeat < 1:
		return fmt.Errorf("Invalid setting for --repeat: '%v', value must be >=1", client.Repeat)
	}
	return nil
}

// Execute runs the program for the 'client' subcommand
func (client *ClientCommand) Execute(args []string) error {
	err := client.ValidateParams(args)
	if err != nil {
		return err
	}
	client.GlobalOpts.SetLogging()
	if client.GlobalOpts.CPUProfileFname != "" {
		f, err := os.Create(client.GlobalOpts.CPUProfileFname)
		if err != nil {
			panic(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	if client.FullSpeed {
		userInfoLogger.Logvf(Always, "Doing playback at full speed")
	} else {
		userInfoLogger.Logvf(Always, "Doing playback at %.2fx speed", client.Speed)
	}

	playbackFileReader, err := NewPlaybackFileReader(client.PlaybackFile, client.Gzip)
	if err != nil {
		return err
	}

	var opChan <-chan *RecordedOp
	var errChan <-chan error

	context := NewExecutionContext(nil, nil, &ExecutionOptions{fullSpeed: client.FullSpeed,
		driverOpsFiltered: playbackFileReader.metadata.DriverOpsFiltered})

	if !client.NoPreprocess {
		opChan, errChan = playbackFileReader.OpChan(1)

		preprocessMap, err := newPreprocessCursorManager(opChan)

		if err != nil {
			return fmt.Errorf("PreprocessMap: %v", err)
		}

		err = <-errChan
		if err != io.EOF {
			return fmt.Errorf("OpChan: %v", err)
		}

		_, err = playbackFileReader.Seek(0, 0)
		if err != nil {
			return err
		}
		context.CursorIDMap = preprocessMap
	}

	opChan, errChan = playbackFileReader.OpChan(client.Repeat)

	if err := Client(context, client.ServerURL, opChan, client.Speed, client.Repeat, client.QueueTime); err != nil {
		userInfoLogger.Logvf(Always, "Play: %v\n", err)
	}

	//handle the error from the errchan
	err = <-errChan
	if err != nil && err != io.EOF {
		userInfoLogger.Logvf(Always, "OpChan: %v", err)
	}
	if client.GlobalOpts.MemProfileFname != "" {
		f, err := os.Create(client.GlobalOpts.MemProfileFname)
		if err != nil {
			panic(err)
		}
		pprof.WriteHeapProfile(f)
		f.Close()
	}
	return nil
}

// Client is responsible for playing ops from a RecordedOp channel to the session.
func Client(context *ExecutionContext,
	serverURL string,
	opChan <-chan *RecordedOp,
	speed float64,
	repeat int,
	queueTime int) error {

	connectionChans := make(map[int64]chan<- *RecordedOp)
	var playbackStartTime, recordingStartTime time.Time
	var connectionID int64
	var opCounter int
	for op := range opChan {
		opCounter++
		if op.Seen.IsZero() {
			return fmt.Errorf("Can't play operation found with zero-timestamp: %#v", op)
		}

		// Every queueGranularity ops make sure that we're no more then
		// QueueTime seconds ahead Which should mean that the maximum that we're
		// ever ahead is QueueTime seconds of ops + queueGranularity more ops.
		// This is so that when we're at QueueTime ahead in the playback file we
		// don't sleep after every read, and generally read and queue
		// queueGranularity number of ops at a time and then sleep until the
		// last read op is QueueTime ahead.
		if !context.fullSpeed {
			if opCounter%queueGranularity == 0 {
				toolDebugLogger.Logvf(DebugHigh, "Waiting to prevent excess buffering with opCounter: %v", opCounter)
				time.Sleep(op.PlayAt.Add(time.Duration(-queueTime) * time.Second).Sub(time.Now()))
			}
		}
		req, err := http.NewRequest("POST", serverURL, bytes.NewBuffer(jsonStr))

	}

	context.ConnectionChansWaitGroup.Wait()

	context.StatCollector.Close()
	toolDebugLogger.Logvf(Always, "%v ops played back in %v seconds over %v connections", opCounter, time.Now().Sub(playbackStartTime), connectionID)
	if repeat > 1 {
		toolDebugLogger.Logvf(Always, "%v ops per generation for %v generations", opCounter/repeat, repeat)
	}
	return nil
}
