package mongoreplay

import (
	"io"

	"github.com/10gen/llmgo/bson"
)

// FilterCommand stores settings for the mongoreplay 'filter' subcommand
type FilterCommand struct {
	GlobalOpts   *Options `no-flag:"true"`
	PlaybackFile string   `description:"path to the playback file to read from" short:"p" long:"playback-file" required:"yes"`
	OutFile      string   `description:"path to the output file to write to" short:"o" long:"output-file"`
	Gzip         bool     `long:"gzip" description:"decompress gzipped input"`
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

	// now write to a new file
	playbackWriter, err := NewPlaybackWriter(filter.OutFile, filter.Gzip)
	if err != nil {
		return err
	}
	defer playbackWriter.Close()

	if err := Filter(opChan, playbackWriter); err != nil {
		userInfoLogger.Logvf(Always, "Play: %v\n", err)
	}

	//handle the error from the errchan
	err = <-errChan
	if err != nil && err != io.EOF {
		userInfoLogger.Logvf(Always, "OpChan: %v", err)
	}
	return nil
}
func Filter(opChan <-chan *RecordedOp, playbackWriter *PlaybackWriter) error {
	for op := range opChan {
		bsonBytes, err := bson.Marshal(op)
		if err != nil {
			return err
		}
		_, err = playbackWriter.Write(bsonBytes)
		if err != nil {
			return err
		}
	}
	return nil
}

func (filter *FilterCommand) ValidateParams(args []string) error {
	return nil
}
