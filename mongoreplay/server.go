package mongoreplay

import (
	"fmt"
	"io"
	"net/http"
	"time"

	mgo "github.com/10gen/llmgo"
)

// ServerCommand stores settings for the mongoreplay 'Server' subcommand
type ServerCommand struct {
	GlobalOpts *Options `no-flag:"true"`
	StatOptions
	Speed     float64 `description:"multiplier for playback speed (1.0 = real-time, .5 = half-speed, 3.0 = triple-speed, etc.)" long:"speed" default:"1.0"`
	URL       string  `short:"h" long:"host" description:"Location of the host to play back against" default:"mongodb://localhost:27017"`
	Collect   string  `long:"collect" description:"Stat collection format; 'format' option uses the --format string" choice:"json" choice:"format" choice:"none" default:"none"`
	FullSpeed bool    `long:"fullSpeed" description:"run the playback as fast as possible"`
}

// ValidateParams validates the settings described in the PlayCommand struct.
func (server *ServerCommand) ValidateParams(args []string) error {
	switch {
	case len(args) > 0:
		return fmt.Errorf("unknown argument: %s", args[0])
	case server.Speed <= 0:
		return fmt.Errorf("Invalid setting for --speed: '%v'", server.Speed)
	}
	return nil
}

// Execute runs the program for the 'Server' subcommand
func (server *ServerCommand) Execute(args []string) error {
	err := server.ValidateParams(args)
	if err != nil {
		return err
	}
	server.GlobalOpts.SetLogging()

	statColl, err := newStatCollector(server.StatOptions, server.Collect, true, true)
	if err != nil {
		return err
	}

	if server.FullSpeed {
		userInfoLogger.Logvf(Always, "Doing playback at full speed")
	} else {
		userInfoLogger.Logvf(Always, "Doing playback at %.2fx speed", server.Speed)
	}

	session, err := mgo.Dial(server.URL)
	if err != nil {
		return err
	}
	session.SetSocketTimeout(0)

	context := NewExecutionContext(statColl, session, &ExecutionOptions{fullSpeed: server.FullSpeed,
		driverOpsFiltered: true})
	session.SetPoolLimit(-1)

	opChan := make(chan *RecordedOp, 100)
	var errChan <-chan error

	go func() {
		if err := Server(context, opChan, server.Speed); err != nil {
			userInfoLogger.Logvf(Always, "Server: %v\n", err)
			return
		}
	}()
	opChanHTTPHandler := getOpChanHTTPHandler(opChan)
	h := http.NewServeMux()

	h.HandleFunc("/", opChanHTTPHandler)  // set router
	err = http.ListenAndServe(":9090", h) // set listen port
	if err != nil {
		return err
	}

	//handle the error from the errchan
	err = <-errChan
	if err != nil && err != io.EOF {
		userInfoLogger.Logvf(Always, "OpChan: %v", err)
	}
	return nil
}

func getOpChanHTTPHandler(opChan chan *RecordedOp) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		recordedOp := &RecordedOp{}
		err := recordedOp.FromReader(r.Body)
		if err != nil {
			fmt.Fprintf(w, "error\n")
			return
		}
		fmt.Fprintf(w, "success\n")

		opChan <- recordedOp
		return
	}
}

// Server is responsible for playing ops from a RecordedOp channel to the session.
func Server(context *ExecutionContext,
	opChan <-chan *RecordedOp,
	speed float64) error {

	connectionChans := make(map[int64]chan<- *RecordedOp)
	var playbackStartTime, recordingStartTime time.Time
	var connectionID int64
	var opCounter int
	for op := range opChan {
		opCounter++
		if op.Seen.IsZero() {
			return fmt.Errorf("Can't play operation found with zero-timestamp: %#v", op)
		}
		if recordingStartTime.IsZero() {
			recordingStartTime = op.Seen
			playbackStartTime = time.Now()
		}

		// opDelta is the difference in time between when the file's recording
		// began and and when this particular op is played. For the first
		// operation in the playback, it's 0.
		opDelta := op.Seen.Sub(recordingStartTime)

		// Adjust the opDelta for playback by dividing it by playback speed setting;
		// e.g. 2x speed means the delta is half as long.
		scaledDelta := float64(opDelta) / (speed)
		op.PlayAt = playbackStartTime.Add(time.Duration(int64(scaledDelta)))

		connectionChan, ok := connectionChans[op.SeenConnectionNum]
		if !ok {
			connectionID++
			connectionChan = context.newExecutionConnection(op.PlayAt, connectionID)
			connectionChans[op.SeenConnectionNum] = connectionChan
		}
		if op.EOF {
			userInfoLogger.Logv(DebugLow, "EOF Seen in playback")
			close(connectionChan)
			delete(connectionChans, op.SeenConnectionNum)
		} else {
			connectionChan <- op
		}
	}
	for connectionNum, connectionChan := range connectionChans {
		close(connectionChan)
		delete(connectionChans, connectionNum)
	}
	toolDebugLogger.Logvf(Info, "Waiting for connections to finish")
	context.ConnectionChansWaitGroup.Wait()

	context.StatCollector.Close()
	toolDebugLogger.Logvf(Always, "%v ops played back in %v seconds over %v connections", opCounter, time.Now().Sub(playbackStartTime), connectionID)
	return nil
}
