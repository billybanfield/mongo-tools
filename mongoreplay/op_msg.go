package mongoreplay

import (
	"io"

	"github.com/10gen/llmgo"
)

// OpMsg sends a diagnostic message to the database. The database sends back a fixed response.
// OpMsg is Deprecated
// http://docs.mongodb.org/meta-driver/latest/legacy/mongodb-wire-protocol/#op-msg

type OpMsgReplyable OpMsg

type OpMsgCursorsRewriteable OpMsg

type OpMsg struct {
	Header MsgHeader
	mgo.MsgOp
	Docs         []bson.Raw
	Latency      time.Duration
	cursorCached bool
	cursorID     int64
}

type payloadType1 struct {
	size       int32
	identifier string
	docs       []bson.Raw
}

// Abbreviated does nothing for an OpMsg
func (op *OpMsg) Abbreviated(chars int) string {
	return ""
}

// OpCode returns the OpCode for the OpMsg.
func (op *OpMsg) OpCode() OpCode {
	return OpCodeMessage
}

// FromReader does nothing for an OpMsg
func (op *OpMsg) FromReader(r io.Reader) error {
	// READ THE  FLAGS
	buf := [4]byte{}
	_, err := io.ReadFull(r, buf)

	op.Flags = getInt32(buf, 0)
	var checksumPresent bool
	if (op.Flags & (1 << 1)) == 1 {
		checksumPresent = true
		checksumLength = 4
	}

	offset := 4
	for op.Header.MessageLength-offset-checkSumLength > 0 {
		sequence, length, err := readSection(r)
		offset += length
	}
	if checksumPresent {
		op.Checksum = getInt32(buf, 0)
	}
}

// Execute does nothing for an OpMsg
func (op *OpMsg) Execute(socket *mgo.MongoSocket) (*ReplyOp, error) {
	return nil, nil
}

// Functions for when this is a reply
func (opMsg *OpMsgReplyable) getCursorID() (int64, error) {
	return 0, nil
}
func (opMsg *OpMsgReplyable) Meta() OpMetadata {
	return OpMetadata{}
}
func (opMsg *OpMsgReplyable) getLatencyMicros() int64 {
	return 0
}
func (opMsg *OpMsgReplyable) getNumReturned() int {
	return 0
}
func (opMsg *OpMsgReplyable) getErrors() []error {
	return []error{}
}

// Functions for when this has rewriteable cursors

func (opMsg *OpMsgCursorsReWriteable) getCursorIDs() ([]int64, error) {
}
func (opMsg *OpMsgCursorsReWriteable) setCursorIDs(cursors []int64) error {
}

func readSection(r io.Reader) (mgo.MsgSection, int, error) {
	// Fetch payload type
	section := mgo.MsgSection{}
	offset := 0
	buf := [4]byte{}
	_, err := io.ReadFull(r, buf[:1])
	if err != nil {
		return mgo.MsgSection{}, 0, err
	}
	offset += 1

	// 2 cases
	// Case 1: Either we have a payload that just contains a bson document (payload == 0)
	// Case 2: We have a payload that contains a size, identifier, and a document (payload == 1)
	//    int32      size;
	//    cstring    identifier;
	//    document*  documents;

	// Case 1: payload == 0
	if (buf[0] && 1) == 0 {
		section.PayloadType = uint8(0)
		docAsSlice, err := ReadDocument(r)
		doc := &bson.Raw{}
		if err != nil {
			return mgo.MsgSection{}, 0, err
		}
		err = bson.Unmarshal(docAsSlice, doc)
		if err != nil {
			return mgo.MsgSection{}, 0, err
		}
		section.Payload = doc
		offset += len(docAsSlice)
		return section, offset, nil
	}
	// Case 2: payload == 1
	section.PayloadType = uint8(1)

	_, err := io.ReadFull(r, buf)
	if err != nil {
		return mgo.MsgSection{}, 0, err
	}
	var payload payloadType1

	// Fetch size
	payload.size = getInt32(buf, 0)
	identifier, err := readCStringFromReader(r)
	if err != nil {
		return mgo.MsgSection{}, 0, err
	}
	offset += len(identifier)
	payload.identifier = string(identifier)

	bytesReadOfPayload := len(identifier) + 4
	//read all the present documents
	for bytesReadOfPayload < size {
		docAsSlice, err := ReadDocument(r)
		if err != nil {
			return mgo.MsgSection{}, 0, err
		}
		docs := &bson.Raw{}
		err = bson.Unmarshal(docsAsSlice, doc)
		if err != nil {
			return mgo.MsgSection{}, 0, err
		}
	}

}
