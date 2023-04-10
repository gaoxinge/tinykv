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
	"fmt"

	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
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
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	r := new(Raft)
	r.id = c.ID
	r.Prs = make(map[uint64]*Progress)
	for _, peer := range c.peers {
		r.Prs[peer] = new(Progress)
	}
	r.electionTimeout = c.ElectionTick
	r.heartbeatTimeout = c.HeartbeatTick
	return r
}

func (r *Raft) newMessage(msgType pb.MessageType, to uint64) pb.Message {
	return pb.Message{
		MsgType: msgType,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	}
}

func (r *Raft) String() string {
	state := ""
	switch r.State {
	case StateFollower:
		state = "Follower"
	case StateCandidate:
		state = "Candidate"
	case StateLeader:
		state = "Leader"
	}
	return fmt.Sprintf("Id: %d, Term: %d, State: %s", r.id, r.Term, state)
}

func messageString(m pb.Message) string {
	return fmt.Sprintf("MsgType: %s, To: %d, From: %d, Reject: %t", m.MsgType, m.To, m.From, m.Reject)
}

func (r *Raft) send(m pb.Message) {
	r.msgs = append(r.msgs, m)
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	return false
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	m := r.newMessage(pb.MessageType_MsgHeartbeat, to)
	r.send(m)
}

func (r *Raft) sendVote(to uint64) {
	m := r.newMessage(pb.MessageType_MsgRequestVote, to)
	r.send(m)
}

func (r *Raft) bcastAppend() {
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.sendAppend(id)
	}
}

func (r *Raft) bcastHeartbeat() {
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.sendHeartbeat(id)
	}
}

func (r *Raft) bcastVote() {
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.sendVote(id)
	}
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	r.heartbeatElapsed++
	r.electionElapsed++

	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		if err := r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat}); err != nil {
			log.Warnf("[%s] step beat msg with error %v", r, err)
		}
	}

	if r.electionElapsed >= r.electionTimeout {
		r.electionElapsed = 0
		if err := r.Step(pb.Message{MsgType: pb.MessageType_MsgHup}); err != nil {
			log.Warnf("[%s] step hup msg with error %v", r, err)
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	if r.Term != term || r.Vote == 0 {
		r.Term = term
		r.Vote = lead
	}
	for peer := range r.Prs {
		r.Prs[peer] = new(Progress)
	}
	r.State = StateFollower
	r.votes = make(map[uint64]bool)
	r.msgs = make([]pb.Message, 0)
	r.Lead = lead
	r.heartbeatElapsed = 0
	r.electionElapsed = 0
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.Term++
	r.Vote = r.id
	for peer := range r.Prs {
		r.Prs[peer] = new(Progress)
	}
	r.State = StateCandidate
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
	r.msgs = make([]pb.Message, 0)
	r.Lead = 0
	r.heartbeatElapsed = 0
	r.electionElapsed = 0
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	for peer := range r.Prs {
		r.Prs[peer] = new(Progress)
	}
	r.State = StateLeader
	r.votes = make(map[uint64]bool)
	r.msgs = make([]pb.Message, 0)
	r.Lead = r.id
	r.heartbeatElapsed = 0
	r.electionElapsed = 0
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	log.Infof("[%s] handle message {%s}", r, messageString(m))

	if m.MsgType != pb.MessageType_MsgHup && m.MsgType != pb.MessageType_MsgBeat {
		if r.Term > m.Term {
			switch m.MsgType {
			case pb.MessageType_MsgRequestVote:
				m = r.newMessage(pb.MessageType_MsgRequestVoteResponse, m.From)
				m.Reject = true
				r.send(m)
			case pb.MessageType_MsgRequestVoteResponse:
				// ignore
			case pb.MessageType_MsgHeartbeat:
				// ignore
			case pb.MessageType_MsgHeartbeatResponse:
				// ignore
			}
			return nil
		} else if r.Term < m.Term {
			switch m.MsgType {
			case pb.MessageType_MsgRequestVote:
				r.becomeFollower(m.Term, 0)
			case pb.MessageType_MsgRequestVoteResponse:
				r.becomeFollower(m.Term, 0)
			case pb.MessageType_MsgHeartbeat:
				r.becomeFollower(m.Term, m.From)
			case pb.MessageType_MsgHeartbeatResponse:
				r.becomeFollower(m.Term, 0)
			}
		} else {
			switch m.MsgType {
			case pb.MessageType_MsgRequestVote:
				// ignore
			case pb.MessageType_MsgRequestVoteResponse:
				// ignore
			case pb.MessageType_MsgHeartbeat:
				r.becomeFollower(m.Term, m.From)
			case pb.MessageType_MsgHeartbeatResponse:
				// ignore
			}
		}
	}

	switch r.State {
	case StateFollower:
		return r.stepFollower(m)
	case StateCandidate:
		return r.stepCandidate(m)
	case StateLeader:
		return r.stepLeader(m)
	}
	return nil
}

func (r *Raft) stepFollower(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.becomeCandidate()
		r.bcastVote()
	case pb.MessageType_MsgBeat:
		// ignore
	case pb.MessageType_MsgRequestVote:
		if r.Vote == 0 || r.Vote == m.From {
			r.Vote = m.From
			m = r.newMessage(pb.MessageType_MsgRequestVoteResponse, m.From)
			m.Reject = false
		} else {
			m = r.newMessage(pb.MessageType_MsgRequestVoteResponse, m.From)
			m.Reject = true
		}
		r.send(m)
	case pb.MessageType_MsgRequestVoteResponse:
		// ignore
	case pb.MessageType_MsgHeartbeat:
		r.electionElapsed = 0
		m = r.newMessage(pb.MessageType_MsgHeartbeatResponse, m.From)
		r.send(m)
	case pb.MessageType_MsgHeartbeatResponse:
		// ignore
	}
	return nil
}

func (r *Raft) stepCandidate(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.becomeCandidate()
		r.bcastVote()
	case pb.MessageType_MsgBeat:
		// ignore
	case pb.MessageType_MsgRequestVote:
		m = r.newMessage(pb.MessageType_MsgRequestVoteResponse, m.From)
		m.Reject = true
		r.send(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.votes[m.From] = !m.Reject
		succCnt := 0
		failCnt := 0
		half := len(r.Prs)/2 + 1
		for _, vote := range r.votes {
			if vote {
				succCnt += 1
			} else {
				failCnt += 1
			}
			if succCnt >= half {
				r.becomeLeader()
			}
			if failCnt >= half {
				r.becomeFollower(r.Term, 0)
			}
		}
	case pb.MessageType_MsgHeartbeat:
		// ignore
	case pb.MessageType_MsgHeartbeatResponse:
		// ignore
	}
	return nil
}

func (r *Raft) stepLeader(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		// ignore
	case pb.MessageType_MsgBeat:
		r.bcastHeartbeat()
	case pb.MessageType_MsgRequestVote:
		m = r.newMessage(pb.MessageType_MsgRequestVoteResponse, m.From)
		m.Reject = true
		r.send(m)
	case pb.MessageType_MsgRequestVoteResponse:
		// ignore
	case pb.MessageType_MsgHeartbeat:
		// ignore
	case pb.MessageType_MsgHeartbeatResponse:
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
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
