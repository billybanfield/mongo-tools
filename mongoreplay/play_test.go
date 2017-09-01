package mongoreplay

import (
	"bytes"
	"io"
	"testing"
	"time"
)

func TestRepeatGeneration(t *testing.T) {
	recOp := &RecordedOp{
		Seen: &PreciseTime{time.Now()},
	}
	var buf bytes.Buffer

	wc := NopWriteCloser(&buf)
	playbackFileWriter, err := playbackFileWriterFromWriteCloser(wc, "", PlaybackFileMetadata{})
	if err != nil {
		t.Fatalf("couldn't create playbackfile writer %v", err)
	}
	err = playbackFileWriter.Encode(recOp)
	if err != nil {
		t.Fatalf("couldn't marshal %v", err)
	}

	reader := bytes.NewReader(buf.Bytes())
	playbackReader, err := playbackFileReaderFromReadSeeker(reader, "")
	if err != nil {
		t.Fatalf("couldn't create playbackfile reader %v", err)
	}

	repeat := 2
	opChan, errChan := playbackReader.OpChan(repeat)
	op1, ok := <-opChan
	if !ok {
		t.Fatalf("read of 0-generation op failed")
	}
	if op1.Generation != 0 {
		t.Fatalf("generation of 0 generation op is %v", op1.Generation)
	}
	op2, ok := <-opChan
	if !ok {
		t.Fatalf("read of 1-generation op failed")
	}
	if op2.Generation != 1 {
		t.Fatalf("generation of 1 generation op is %v", op2.Generation)
	}
	_, ok = <-opChan
	if ok {
		t.Fatalf("Successfully read past end of op chan")
	}
	err = <-errChan
	if err != io.EOF {
		t.Fatalf("should have eof at end, but got %v", err)
	}
}

func TestPlayOpEOF(t *testing.T) {
	ops := []RecordedOp{{
		Seen: &PreciseTime{time.Now()},
	}, {
		Seen: &PreciseTime{time.Now()},
		EOF:  true,
	}}
	var buf bytes.Buffer

	wc := NopWriteCloser(&buf)
	playbackFileWriter, err := playbackFileWriterFromWriteCloser(wc, "", PlaybackFileMetadata{})
	if err != nil {
		t.Fatalf("couldn't create playbackfile writer %v", err)
	}
	for _, op := range ops {
		err := playbackFileWriter.Encode(op)
		if err != nil {
			t.Fatalf("couldn't marshal op %v", err)
		}
	}

	reader := bytes.NewReader(buf.Bytes())
	playbackReader, err := playbackFileReaderFromReadSeeker(reader, "")
	if err != nil {
		t.Fatalf("couldn't create playbackfile reader %v", err)
	}

	repeat := 2
	opChan, errChan := playbackReader.OpChan(repeat)

	op1, ok := <-opChan
	if !ok {
		t.Fatalf("read of op1 failed")
	}
	if op1.EOF {
		t.Fatalf("op1 should not be an EOF op")
	}
	op2, ok := <-opChan
	if !ok {
		t.Fatalf("read op2 failed")
	}
	if op2.EOF {
		t.Fatalf("op2 should not be an EOF op")
	}
	op3, ok := <-opChan
	if !ok {
		t.Fatalf("read of op3 failed")
	}
	if !op3.EOF {
		t.Fatalf("op3 is not an EOF op")
	}

	_, ok = <-opChan
	if ok {
		t.Fatalf("Successfully read past end of op chan")
	}
	err = <-errChan
	if err != io.EOF {
		t.Fatalf("should have eof at end, but got %v", err)
	}
}
