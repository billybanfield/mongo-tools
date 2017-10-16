package mongoreplay

import (
	"io"

	"github.com/10gen/llmgo"
	"github.com/10gen/llmgo/bson"
)

// MsgOp sends a diagnostic message to the database. The database sends back a fixed response.
// MsgOp is Deprecated
// http://docs.mongodb.org/meta-driver/latest/legacy/mongodb-wire-protocol/#op-msg

type MsgOpReplyable MsgOp

type MsgOpCursorsRewriteable MsgOp

type MsgOp struct {
	Header MsgHeader
	mgo.MsgOp
	/*
		Docs         []bson.Raw
		Latency      time.Duration
		cursorCached bool
		cursorID     int64
	*/
}

// Abbreviated does nothing for an MsgOp
func (op *MsgOp) Abbreviated(chars int) string {
	return ""
}

// OpCode returns the OpCode for the MsgOp.
func (op *MsgOp) OpCode() OpCode {
	return OpCodeMessage
}

// FromReader does nothing for an MsgOp
func (op *MsgOp) FromReader(r io.Reader) error {
	// READ THE  FLAGS
	buf := [4]byte{}
	_, err := io.ReadFull(r, buf[:])
	if err != nil {
		return err
	}

	var checksumLength int
	op.Flags = uint32(getInt32(buf[:], 0))
	var checksumPresent bool
	if (op.Flags & (1 << 1)) == 1 {
		checksumPresent = true
		checksumLength = 4
	}

	offset := 4
	for int(op.Header.MessageLength)-offset-checksumLength > 0 {
		section, length, err := readSection(r)
		if err != nil {
			return err
		}
		op.Sections = append(op.Sections, section)
		offset += length
	}
	if checksumPresent {
		_, err := io.ReadFull(r, buf[:])
		if err != nil {
			return err
		}
		op.Checksum = uint32(getInt32(buf[:], 0))
	}
	return nil
}

// Execute does nothing for an MsgOp
func (op *MsgOp) Execute(socket *mgo.MongoSocket) (Replyable, error) {
	return nil, nil
}

// Functions for when this is a reply
func (msgOp *MsgOpReplyable) getCursorID() (int64, error) {
	return 0, nil
}
func (msgOp *MsgOp) Meta() OpMetadata {
	return OpMetadata{}
}
func (msgOp *MsgOpReplyable) getLatencyMicros() int64 {
	return 0
}
func (msgOp *MsgOpReplyable) getNumReturned() int {
	return 0
}
func (msgOp *MsgOpReplyable) getErrors() []error {
	return []error{}
}

// Functions for when this has rewriteable cursors

func (msgOp *MsgOpCursorsRewriteable) getCursorIDs() ([]int64, error) {
	return []int64{}, nil
}
func (msgOp *MsgOpCursorsRewriteable) setCursorIDs(cursors []int64) error {
	return nil
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
	if (buf[0] & 1) == 0 {
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
		section.Data = doc
		offset += len(docAsSlice)
		return section, offset, nil
	}
	// Case 2: payload == 1
	section.PayloadType = uint8(1)

	_, err = io.ReadFull(r, buf[:])
	if err != nil {
		return mgo.MsgSection{}, 0, err
	}

	var payload mgo.PayloadType1

	// Fetch size
	payload.Size = getInt32(buf[:], 0)
	identifier, err := readCStringFromReader(r)
	if err != nil {
		return mgo.MsgSection{}, 0, err
	}
	offset += len(identifier)
	payload.Identifier = string(identifier)

	bytesReadOfPayload := len(identifier) + 4

	docs := []bson.Raw{}
	//read all the present documents
	for bytesReadOfPayload < int(payload.Size) {
		docAsSlice, err := ReadDocument(r)
		if err != nil {
			return mgo.MsgSection{}, 0, err
		}

		doc := bson.Raw{}
		err = bson.Unmarshal(docAsSlice, &doc)
		if err != nil {
			return mgo.MsgSection{}, 0, err
		}
		docs = append(docs, doc)
		offset += len(docAsSlice)
		bytesReadOfPayload += len(docAsSlice)
	}
	payload.Docs = docs
	section.Data = payload

	return section, offset, nil
}
