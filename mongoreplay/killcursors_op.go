package mongoreplay

import (
	"fmt"
	"io"

	mgo "github.com/10gen/llmgo"
)

// KillCursorsOp is used to close an active cursor in the database. This is necessary
// to ensure that database resources are reclaimed at the end of the query.
// http://docs.mongodb.org/meta-driver/latest/legacy/mongodb-wire-protocol/#op-kill-cursors
type KillCursorsOp struct {
	Header MsgHeader
	mgo.KillCursorsOp
}

// Meta returns metadata about the KillCursorsOp, useful for analysis of traffic.
func (op *KillCursorsOp) Meta() OpMetadata {
	return OpMetadata{"killcursors", "", "", op.CursorIds}
}

func (op *KillCursorsOp) String() string {
	return fmt.Sprintf("KillCursorsOp %v", op.CursorIds)
}

// Abbreviated returns a serialization of the KillCursorsOp, abbreviated so it
// doesn't exceed the given number of characters.
func (op *KillCursorsOp) Abbreviated(chars int) string {
	return fmt.Sprintf("%v", op)
}

// OpCode returns the OpCode for the KillCursorsOp.
func (op *KillCursorsOp) OpCode() OpCode {
	return OpCodeKillCursors
}

func (op *KillCursorsOp) getCursorIDs() ([]int64, error) {
	return op.KillCursorsOp.CursorIds, nil
}
func (op *KillCursorsOp) setCursorIDs(cursorIDs []int64) error {
	op.KillCursorsOp.CursorIds = cursorIDs
	return nil
}

// FromReader extracts data from a serialized KillCursorsOp into its concrete
// structure.
func (op *KillCursorsOp) FromReader(r io.Reader) error {
	var b [8]byte
	_, err := io.ReadFull(r, b[:]) //skip ZERO and grab numberOfCursors
	if err != nil {
		return err
	}

	numCursors := uint32(getInt32(b[4:], 0))
	var i uint32
	for i = 0; i < numCursors; i++ {
		if _, err := io.ReadFull(r, b[:]); err != nil {
			return nil
		}
		op.CursorIds = append(op.CursorIds, getInt64(b[:], 0))
	}
	return nil
}

func (op *KillCursorsOp) FromSlice(s []byte) error {
	offset := 4
	if len(s) < offset+4 {
		return fmt.Errorf("unable to parse KillCursor: slice doesn't contain numCursors")
	}
	numCursors := uint32(getInt32(s[offset:], 0))
	offset += 4
	var i uint32
	for i = 0; i < numCursors; i++ {
		if len(s) < offset+8 {
			return fmt.Errorf("unable to parse KillCursor: slice not long enough to contain cursor id")
		}
		cursorId := getInt64(s[offset:], 0)
		op.CursorIds = append(op.CursorIds, cursorId)
		offset += 8
	}
	return nil
}

// Execute performs the KillCursorsOp on a given session, yielding the reply
// when successful (and an error otherwise).
func (op *KillCursorsOp) Execute(socket *mgo.MongoSocket) (Replyable, error) {
	if err := mgo.ExecOpWithoutReply(socket, &op.KillCursorsOp); err != nil {
		return nil, err
	}

	return nil, nil
}
