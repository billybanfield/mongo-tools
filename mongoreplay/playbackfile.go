package mongoreplay

import (
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/10gen/llmgo/bson"
	"github.com/mongodb/mongo-tools/common/util"
)

// PlaybackFileReader stores the necessary information for a playback source,
// which is just an io.ReadCloser.
type PlaybackFileReader struct {
	io.ReadSeeker
}

// PlaybackFileWriter stores the necessary information for a playback destination,
// which is an io.WriteCloser and its location.
type PlaybackFileWriter struct {
	io.WriteCloser
	fname string
}

// GzipReadSeeker wraps an io.ReadSeeker for gzip reading
type GzipReadSeeker struct {
	readSeeker io.ReadSeeker
	*gzip.Reader
}

// NewPlaybackFileReader initializes a new PlaybackFileReader
func NewPlaybackFileReader(filename string, gzip bool) (*PlaybackFileReader, error) {
	var readSeeker io.ReadSeeker

	readSeeker, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	if gzip {
		readSeeker, err = NewGzipReadSeeker(readSeeker)
		if err != nil {
			return nil, err
		}
	}

	pfReader := &PlaybackFileReader{
		ReadSeeker: readSeeker,
	}

	return pfReader, nil
}

// NextRecordedOp iterates through the PlaybackFileReader to yield the next
// RecordedOp. It returns io.EOF when successfully complete.
func (file *PlaybackFileReader) NextRecordedOp() (*RecordedOp, error) {
	buf, err := ReadDocument(file)
	if err != nil {
		if err != io.EOF {
			err = fmt.Errorf("ReadDocument Error: %v", err)
		}
		return nil, err
	}

	doc := new(RecordedOp)
	err = bson.Unmarshal(buf, doc)
	if err != nil {
		return nil, fmt.Errorf("Unmarshal RecordedOp Error: %v\n", err)
	}

	return doc, nil
}

// NewPlaybackFileWriter initializes a new PlaybackFileWriter
func NewPlaybackFileWriter(playbackFileName string, isGzipWriter bool) (*PlaybackFileWriter, error) {
	pbWriter := &PlaybackFileWriter{
		fname: playbackFileName,
	}
	toolDebugLogger.Logvf(DebugLow, "Opening playback file %v", playbackFileName)
	file, err := os.Create(pbWriter.fname)
	if err != nil {
		return nil, fmt.Errorf("error opening playback file to write to: %v", err)
	}
	if isGzipWriter {
		pbWriter.WriteCloser = &util.WrappedWriteCloser{gzip.NewWriter(file), file}
	} else {
		pbWriter.WriteCloser = file
	}

	return pbWriter, nil
}

// NewGzipReadSeeker initializes a new GzipReadSeeker
func NewGzipReadSeeker(rs io.ReadSeeker) (*GzipReadSeeker, error) {
	gzipReader, err := gzip.NewReader(rs)
	if err != nil {
		return nil, err
	}
	return &GzipReadSeeker{rs, gzipReader}, nil
}

// Seek sets the offset for the next Read, and can only seek to the
// beginning of the file.
func (g *GzipReadSeeker) Seek(offset int64, whence int) (int64, error) {
	if whence != 0 || offset != 0 {
		return 0, fmt.Errorf("GzipReadSeeker can only seek to beginning of file")
	}
	_, err := g.readSeeker.Seek(offset, whence)
	if err != nil {
		return 0, err
	}
	g.Reset(g.readSeeker)
	return 0, nil
}

// OpChan runs a goroutine that will read and unmarshal recorded ops
// from a file and push them in to a recorded op chan. Any errors encountered
// are pushed to an error chan. Both the recorded op chan and the error chan are
// returned by the function.
// The error chan won't be readable until the recorded op chan gets closed.
func (pfReader *PlaybackFileReader) OpChan(repeat int) (<-chan *RecordedOp, <-chan error) {
	ch := make(chan *RecordedOp)
	e := make(chan error)

	var last time.Time
	var first time.Time
	var loopDelta time.Duration
	go func() {
		defer close(e)
		e <- func() error {
			defer close(ch)
			toolDebugLogger.Logv(Info, "Beginning playback file read")
			for generation := 0; generation < repeat; generation++ {
				_, err := pfReader.Seek(0, 0)
				if err != nil {
					return fmt.Errorf("PlaybackFile Seek: %v", err)
				}

				var order int64
				for {
					recordedOp, err := pfReader.NextRecordedOp()
					if err != nil {
						if err == io.EOF {
							break
						}
						return err
					}
					last = recordedOp.Seen.Time
					if first.IsZero() {
						first = recordedOp.Seen.Time
					}
					recordedOp.Seen.Time = recordedOp.Seen.Add(loopDelta)
					recordedOp.Generation = generation
					recordedOp.Order = order
					// We want to suppress EOF's unless you're in the last
					// generation because all of the ops for one connection
					// across different generations get executed in the same
					// session. We don't want to close the session until the
					// connection closes in the last generation.
					if !recordedOp.EOF || generation == repeat-1 {
						ch <- recordedOp
					}
					order++
				}
				toolDebugLogger.Logvf(DebugHigh, "generation: %v", generation)
				loopDelta += last.Sub(first)
				first = time.Time{}
				continue
			}
			return io.EOF
		}()
	}()
	return ch, e
}
