package mongoreplay

import (
	mgo "github.com/10gen/llmgo"
)

// TesterCommand stores settings for the mongoreplay 'test' subcommand
type TesterCommand struct {
	GlobalOpts *Options `no-flag:"true"`
	URL        string   `short:"h" long:"host" description:"Location of the host to play back against" default:"mongodb://localhost:27017"`
	StatOptions
}

func (tester *TesterCommand) Execute(args []string) error {
	conns := 4000
	sessions := make([]*mgo.Session, conns)

	for i := 0; i < conns; i++ {
		session, err := mgo.Dial(tester.URL)
		if err != nil {
			panic(err)
		}
		sessions[i] = session
	}
	for _, session := range sessions {
		session.Close()
	}
	return nil
}

func (tester *TesterCommand) Execute(args []string) error {
	conns := 4000
	sessions := make([]*mgo.Session, conns)

	for i := 0; i < conns; i++ {
		socket, err := session.AcquireT(tester.URL)
		if err != nil {
			panic(err)
		}
		sessions[i] = session
	}
	for _, session := range sessions {
		session.Close()
	}
	return nil
}
