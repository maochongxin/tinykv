// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"math/rand"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/sirupsen/logrus"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int

	randomizedElectionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	rand *rand.Rand
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}

	return &Raft{
		id:   c.ID,
		Vote: None,
		Term: 0,
		RaftLog: &RaftLog{
			storage: c.Storage,
			applied: c.Applied,
		},
		rand:  rand.New(rand.NewSource(int64(c.ID))),
		State: StateFollower,
	}
}

func (r *Raft) reset(term uint64) {
	if r.Term != term {
		r.Term = term
		r.Vote = None
	}
	r.Lead = None

	r.electionElapsed = 0
	r.heartbeatElapsed = 0

	r.votes = make(map[uint64]bool)
	for id := range r.Prs {
		r.Prs[id] = &Progress{Next: r.RaftLog.LastIndex() + 1}
		if id == r.id {
			r.Prs[id].Match = r.RaftLog.LastIndex()
		}
	}
	r.PendingConfIndex = None
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	pr := r.Prs[to]

	m := pb.Message{
		To: to,
	}

	term, errt := r.RaftLog.Term(pr.Next - 1)
	var ents []*pb.Entry
	if pr.Next < r.RaftLog.lastIndex()+1 {
		for i := pr.Next; i < r.RaftLog.lastIndex()+1; i++ {
			ents = append(ents, &r.RaftLog.entries[i])
		}
	}
	if errt != nil {
		m.MsgType = pb.MessageType_MsgSnapshot
		snapshot, err := r.RaftLog.storage.Snapshot()
		if err != nil {
			if err != ErrSnapshotTemporarilyUnavailable {
				logrus.Debugf("%x failed to send snapshot to %x because snapshot is temporarily unavailable", r.id, to)
				return false
			}
			panic(err)
		}
		if IsEmptySnap(&snapshot) {
			panic("need non-empty snapshot")
		}
		m.Snapshot = &snapshot
		sindex, sterm := snapshot.Metadata.Index, snapshot.Metadata.Term
		logrus.Debugf("%x [first index: %d, commit: %d] sent snapshot[index: %d, term: %d] to %x [%v]", r.id, r.RaftLog.firstIndex(), r.RaftLog.committed, sindex, sterm, to, pr)
	} else {
		m.MsgType = pb.MessageType_MsgAppend
		m.Index = pr.Next - 1
		m.LogTerm = term
		m.Entries = ents
		m.Commit = r.RaftLog.committed
		if len(m.Entries) != 0 {

		}
	}
	r.send(m)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	commit := min(r.Prs[to].Match, r.RaftLog.committed)
	m := pb.Message{
		To:      to,
		MsgType: pb.MessageType_MsgHeartbeat,
		Commit:  commit,
	}
	r.send(m)
}

func (r *Raft) tickElection() {
	if _, ok := r.Prs[r.id]; !ok {
		r.electionElapsed = 0
		return
	}
	r.electionElapsed++
	if d := r.electionElapsed - r.electionTimeout; d > 0 && d > r.rand.Int()%r.electionTimeout {
		r.electionElapsed = 0
		r.Step(pb.Message{From: r.id, MsgType: pb.MessageType_MsgHup})
	}
}

func (r *Raft) tickHeartbeat() {
	r.heartbeatElapsed++
	r.electionElapsed++
	if r.electionElapsed >= r.electionTimeout {
	}
	if r.State != StateLeader {
		return
	}
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		r.Step(pb.Message{From: r.id, MsgType: pb.MessageType_MsgBeat})
	}
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	switch r.State {
	case StateCandidate:
		r.tickElection()
	case StateFollower:
		r.tickElection()
	case StateLeader:
		r.tickHeartbeat()
	}
}

func (r *Raft) appendEntry(entries ...*pb.Entry) {
	lastIndex := r.RaftLog.LastIndex()
	for i, entrie := range entries {
		entrie.Term = r.Term
		entrie.Index = lastIndex + 1 + uint64(i)
	}

	// r.RaftLog.entries.
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	r.reset(term)
	r.Lead = lead
	r.State = StateFollower
	logrus.Infof("%x became follower at term %d", r.id, r.Term)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	if r.State == StateLeader {
		panic("invalid transition, [leader -> candidate]")
	}
	r.reset(r.Term + 1)
	r.State = StateCandidate
	logrus.Infof("%x became candidate at term %d", r.id, r.Term)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	if r.State == StateFollower {
		panic("invalid transition [follower -> leader]")
	}

	r.reset(r.Term)
	r.Lead = r.id
	r.State = StateLeader
	var ents []pb.Entry
	logrus.Info(r.RaftLog)
	logrus.Info(r.RaftLog.committed, r.RaftLog.lastIndex())
	if r.RaftLog.committed+1 < r.RaftLog.lastIndex() {
		ents = r.RaftLog.entries[r.RaftLog.committed+1 : r.RaftLog.LastIndex()+1]
	}

	for _, e := range ents {
		if e.EntryType != pb.EntryType_EntryConfChange {
			continue
		}
		if r.PendingConfIndex > e.Index {
			break
		}
	}
	r.appendEntry(&pb.Entry{Data: nil})
	logrus.Infof("%x became leader at term %d", r.id, r.Term)
}

func (r *Raft) campaign() {
	r.becomeCandidate()
	if r.pool(r.id, true) >= r.quorum() {
		r.becomeLeader()
		return
	}
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		term, err := r.RaftLog.Term(r.RaftLog.lastIndex())
		if err != nil {
			panic(err)
		}
		logrus.Infof("%x [logterm: %d, index: %d] sent vote request to %x at term %d", r.id, term, r.RaftLog.LastIndex(), id, r.Term)
	}
}

func (r *Raft) quorum() int {
	return len(r.Prs)/2 + 1
}

func (r *Raft) pool(id uint64, vote bool) (granted int) {
	if vote {
		logrus.Infof("%x received vote from %x at term %d", r.id, id, r.Term)
	} else {
		logrus.Infof("%x received vote rejection from %x at term %d", r.id, id, r.Term)
	}

	if _, ok := r.votes[id]; !ok {
		r.votes[id] = vote
	}
	for _, v := range r.votes {
		if v {
			granted++
		}
	}
	return granted
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	if m.MsgType == pb.MessageType_MsgHup {
		if r.State != StateLeader {
			logrus.Infof("%x is starting a new election at term %d", r.id, r.Term)
			r.campaign()
		} else {
			logrus.Debugf("%x ignoring MsgHup because already leader", r.id)
		}
		return nil
	}

	switch {
	case m.Term == 0:
		// local message
	case m.Term > r.Term:
		lead := m.From
		if m.MsgType == pb.MessageType_MsgRequestVote {
			lead = None
		}
		logrus.Infof("%x [term: %d] received a %s message with higher term from %x [term: %d]",
			r.id, r.Term, m.MsgType, m.From, m.Term)
		r.becomeFollower(m.Term, lead)
	case m.Term < r.Term:
		logrus.Infof("%x [term: %d] ignored a %s message with lower term from %x [term: %d]",
			r.id, r.Term, m.MsgType, m.From, m.Term)
		return nil
	}

	switch r.State {
	case StateFollower:
		switch m.MsgType {
		case pb.MessageType_MsgPropose:
			if r.Lead == None {
				logrus.Infof("%x no leader at term %d; dropping proposal", r.id, r.Term)
				return nil
			}
			m.To = r.Lead
			r.send(m)
		case pb.MessageType_MsgAppend:
			r.electionElapsed = 0
			r.Lead = m.From
			r.handleAppendEntries(m)
		case pb.MessageType_MsgHeartbeat:
			r.electionElapsed = 0
			r.Lead = m.From
			r.handleHeartbeat(m)
		case pb.MessageType_MsgSnapshot:
		case pb.MessageType_MsgRequestVote:
			if (r.Vote == None || r.Vote == m.From) &&
				((m.Term > r.RaftLog.lastTerm()) || (m.Term == r.RaftLog.lastTerm() && m.Index >= r.RaftLog.lastIndex())) {
				r.electionElapsed = 0
				logrus.Infof("%x [logterm: %d, index: %d, vote: %x] voted for %x [logterm: %d, index: %d] at term %d",
					r.id, r.RaftLog.lastTerm(), r.RaftLog.lastIndex(), r.Vote, m.From, m.LogTerm, m.Index, r.Term)
				r.Vote = m.From
				r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgRequestVoteResponse})
			} else {
				logrus.Infof("%x [logterm: %d, index: %d, vote: %x] rejected voted for %x [logterm: %d, index: %d] at term %d",
					r.id, r.RaftLog.lastTerm(), r.RaftLog.lastIndex(), r.Vote, m.From, m.LogTerm, m.Index, r.Term)
				r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgRequestVoteResponse, Reject: true})
			}
		}
	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgPropose:
			return errors.New("")
		case pb.MessageType_MsgAppend:
			r.becomeFollower(r.Term, m.From)
			r.handleAppendEntries(m)
		case pb.MessageType_MsgHeartbeat:
			r.becomeFollower(r.Term, m.From)
			r.handleHeartbeat(m)
		case pb.MessageType_MsgSnapshot:
			r.becomeFollower(m.Term, m.From)
			r.handleSnapshot(m)
		case pb.MessageType_MsgRequestVote:
			logrus.Infof("%x [logterm: %d, index: %d, vote: %x] rejected vote from %x [logterm: %d, index: %d] at term %d",
				r.id, r.RaftLog.lastTerm(), r.RaftLog.lastIndex(), r.Vote, m.From, m.LogTerm, m.Index, r.Term)
		case pb.MessageType_MsgRequestVoteResponse:
			garnted := r.pool(m.From, !m.Reject)
			if garnted >= r.quorum() {
				r.becomeLeader()
				r.broadcastAppend()
			} else {
				r.becomeFollower(r.Term, None)
			}
		}
	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgBeat:
			r.broadcastHeartbeat()
			return nil
		case pb.MessageType_MsgPropose:
			if len(m.Entries) == 0 {
				logrus.Panicf("%x stepped empty MsgProp", r.id)
			}
			if _, ok := r.Prs[r.id]; !ok {
				return nil
			}
			for index, entry := range m.Entries {
				if entry.EntryType == pb.EntryType_EntryConfChange {
					if r.PendingConfIndex < entry.Index {
						m.Entries[index] = &pb.Entry{EntryType: pb.EntryType_EntryNormal}
					}
				}
			}
			r.appendEntry(m.Entries...)
			r.broadcastAppend()
			return nil
		case pb.MessageType_MsgRequestVote:
			logrus.Infof("%x [logterm: %d, index: %d, vote: %x] rejected vote from %x [logterm: %d, index: %d] at term %d",
				r.id, r.RaftLog.lastTerm(), r.RaftLog.lastIndex(), r.Vote, m.From, m.LogTerm, m.Index, r.Term)
			r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgRequestVoteResponse, Reject: true})
			return nil
		}
		pr, prOk := r.Prs[m.From]
		if !prOk {
			logrus.Debugf("no process available for %x", m.From)
			return nil
		}
		switch m.MsgType {
		case pb.MessageType_MsgAppendResponse:
			if m.Reject {
				logrus.Debugf("%x received msgApp rejection from %x for index %d", r.id, m.From, m.Index)

			} else {

			}
		case pb.MessageType_MsgHeartbeatResponse:
			if pr.Match < r.RaftLog.lastIndex() {
				r.sendAppend(m.From)
			}
		}
	}
	return nil
}

func (r *Raft) send(m pb.Message) {
	m.From = r.id
	if m.MsgType != pb.MessageType_MsgPropose {
		m.Term = r.Term
	}
	r.msgs = append(r.msgs, m)
}

func (r *Raft) broadcastAppend() {
	for id := range r.Prs {
		if id != r.id {
			r.sendAppend(id)
		}
	}
}

func (r *Raft) broadcastHeartbeat() {
	for id := range r.Prs {
		if id != r.id {
			r.sendHeartbeat(id)
		}
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	if m.MsgType == pb.MessageType_MsgAppend {

	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	if m.MsgType == pb.MessageType_MsgBeat {
		r.becomeFollower(m.Term, m.From)
	} else {
		r.becomeFollower(m.Term, None)
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
