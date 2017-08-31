package mongoreplay

import (
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/10gen/llmgo/bson"
)

// FilterCommand stores settings for the mongoreplay 'filter' subcommand
type FilterCommand struct {
	GlobalOpts      *Options `no-flag:"true"`
	PlaybackFile    string   `description:"path to the playback file to read from" short:"p" long:"playback-file" required:"yes"`
	OutFile         string   `description:"path to the output file to write to" short:"o" long:"outputFile"`
	SplitFilePrefix string   `description:"prefix file name to use for the output files being written when splitting traffic" long:"outfilePrefix"`
	Split           int      `description:"split the traffic into n files with roughly equal numbers of connecitons in each" default:"1" long:"split"`
	RemoveDriverOps bool     `description:"remove driver issued operations from the playback" long:"removeDriverOps"`
	Gzip            bool     `long:"gzip" description:"decompress gzipped input"`
}

// Execute runs the program for the 'filter' subcommand
func (filter *FilterCommand) Execute(args []string) error {
	err := filter.ValidateParams(args)
	if err != nil {
		return err
	}
	filter.GlobalOpts.SetLogging()

	playbackFileReader, err := NewPlaybackFileReader(filter.PlaybackFile, filter.Gzip)
	if err != nil {
		return err
	}
	opChan, errChan := NewOpChanFromFile(playbackFileReader, 1)

	outfiles := make([]*PlaybackWriter, filter.Split)
	if filter.Split == 1 {
		playbackWriter, err := NewPlaybackWriter(filter.OutFile, filter.Gzip)
		if err != nil {
			return err
		}
		outfiles[0] = playbackWriter
	} else {
		for i := 0; i < filter.Split; i++ {
			playbackWriter, err := NewPlaybackWriter(
				fmt.Sprintf("%s%02d.playback", filter.SplitFilePrefix, i), filter.Gzip)
			if err != nil {
				return err
			}
			outfiles[i] = playbackWriter
			defer playbackWriter.Close()
		}
	}

	if err := Filter(opChan, outfiles, filter.RemoveDriverOps); err != nil {
		userInfoLogger.Logvf(Always, "Play: %v\n", err)
	}

	//handle the error from the errchan
	err = <-errChan
	if err != nil && err != io.EOF {
		userInfoLogger.Logvf(Always, "OpChan: %v", err)
	}
	return nil
}

func Filter(opChan <-chan *RecordedOp,
	outfiles []*PlaybackWriter,
	removeDriverOps bool) error {

	opWriters := make([]chan<- *RecordedOp, len(outfiles))
	errChan := make(chan error)
	wg := &sync.WaitGroup{}

	for i := range outfiles {
		opWriters[i] = newParallelPlaybackWriter(outfiles[i], errChan, wg)
	}

	for op := range opChan {
		if removeDriverOps {
			parsedOp, err := op.RawOp.Parse()
			if err != nil {
				return err
			}
			if IsDriverOp(parsedOp) {
				continue
			}
		}
		// Determine which one to write it to
		fileNum := op.SeenConnectionNum % int64(len(outfiles))
		opWriters[fileNum] <- op
	}

	for _, opWriter := range opWriters {
		close(opWriter)
	}
	wg.Wait()
	close(errChan)

	var hasError bool
	for err := range errChan {
		hasError = true
		userInfoLogger.Logvf(Always, "error: %s", err)
	}
	if hasError {
		return fmt.Errorf("errors encountered while running filter")
	}

	return nil
}

func newParallelPlaybackWriter(outfile *PlaybackWriter,
	errChan chan<- error, wg *sync.WaitGroup) chan<- *RecordedOp {
	var didWriteOp bool

	inputOpChan := make(chan *RecordedOp, 1000)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for op := range inputOpChan {
			bsonBytes, err := bson.Marshal(op)
			if err != nil {
				errChan <- err
				return
			}
			_, err = outfile.Write(bsonBytes)
			if err != nil {
				errChan <- err
				return
			}
			didWriteOp = true
		}
		if !didWriteOp {
			userInfoLogger.Logvf(Always, "no connections written to file %s, removing", outfile.fname)
			err := os.Remove(outfile.fname)
			if err != nil {
				errChan <- err
				return
			}
		}
	}()
	return inputOpChan
}

func (filter *FilterCommand) ValidateParams(args []string) error {
	if filter.Split < 1 {
		return fmt.Errorf("must be a positive number of files to split into")
	}
	if filter.Split > 1 && filter.SplitFilePrefix == "" {
		return fmt.Errorf("must specify a filename prefix when splitting traffic")
	}
	if filter.Split > 1 && filter.OutFile != "" {
		return fmt.Errorf("must not specify an output file name when splitting traffic" +
			"instead only specify a file name prefix")
	}
	if filter.Split == 1 && filter.OutFile == "" {
		return fmt.Errorf("must specify an output file")
	}
	return nil
}
