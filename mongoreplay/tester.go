package mongoreplay

import ()

// TesterCommand stores settings for the mongoreplay 'test' subcommand
type TesterCommand struct {
	GlobalOpts *Options `no-flag:"true"`
	URL        string   `short:"h" long:"host" description:"Location of the host to play back against" default:"mongodb://localhost:27017"`
	StatOptions
}

func (tester *TesterCommand) Execute(args []string) error {
}
