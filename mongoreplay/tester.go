package mongoreplay

import (
	"fmt"
	"io"
)

// TesterCommand stores settings for the mongoreplay 'test' subcommand
type TesterCommand struct {
	GlobalOpts   *Options `no-flag:"true"`
	PlaybackFile string   `description:"path to the playback file to play from" short:"p" long:"playback-file" required:"yes"`
	StatOptions
}

func (tester *TesterCommand) Execute(args []string) error {
	fmt.Println("hello")
	playbackFileReader, err := NewPlaybackFileReader(tester.PlaybackFile, false)
	if err != nil {
		return err
	}

	var opChan <-chan *RecordedOp
	var errChan <-chan error

	opChan, errChan = NewOpChanFromFile(playbackFileReader, 1)

	for range opChan {
		/*

			put the relevant test thing to do here

		*/

	}
	//handle the error from the errchan
	err = <-errChan
	if err != nil && err != io.EOF {
		userInfoLogger.Logvf(Always, "OpChan: %v", err)
	}
	return nil
}
