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

	randomElectionTimeout int
	granted               int
	rejected              int
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	hardState, confState, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}
	r := &Raft{
		id:               c.ID,
		Term:             hardState.Term,
		Vote:             hardState.Vote,
		RaftLog:          newLog(c.Storage),
		Prs:              make(map[uint64]*Progress),
		State:            StateFollower,
		votes:            make(map[uint64]bool),
		msgs:             make([]pb.Message, 0),
		Lead:             None,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
	}
	lastIndex := r.RaftLog.LastIndex()
	if c.peers == nil {
		c.peers = confState.Nodes
	}
	for _, id := range c.peers {
		r.Prs[id] = &Progress{Match: 0, Next: lastIndex + 1}
		if id == r.id {
			r.Prs[id].Match = lastIndex
		}
	}
	//存在测试给你ID但是peers为nil，不能这么写
	//r.Prs[r.id].Match = lastIndex
	r.becomeFollower(0, None)
	return r
}

func (r *Raft) softState() *SoftState {
	return &SoftState{
		Lead:      r.Lead,
		RaftState: r.State,
	}
}

func (r *Raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}

func (r *Raft) appendEntry(es ...*pb.Entry) {
	entries := make([]pb.Entry, 0)
	lastIndex := r.RaftLog.LastIndex()
	for index, e := range es {
		entries = append(entries, pb.Entry{
			EntryType: e.EntryType,
			Term:      r.Term,
			Index:     lastIndex + uint64(index) + 1,
			Data:      e.Data,
		})
	}
	r.RaftLog.appendEntries(entries...)
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
}

func (r *Raft) bcastAppend() {
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.sendAppend(id)
	}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	preLogIndex := r.Prs[to].Next - 1
	preLogTerm, err := r.RaftLog.Term(preLogIndex)
	if err != nil {
		if err == ErrCompacted {
			r.sendSnapshot(to)
		}

		//log.Debugf("preLogIndex nil :%v", preLogIndex)
		return false
	}
	//log.Debugf("entries :%v, lastIndex:%v", r.RaftLog.entries, r.RaftLog.LastIndex())
	entries := r.RaftLog.Entries(preLogIndex+1, r.RaftLog.LastIndex()+1)
	sendEntries := make([]*pb.Entry, 0)
	for _, entry := range entries {
		sendEntries = append(sendEntries, &pb.Entry{
			EntryType: entry.EntryType,
			Term:      entry.Term,
			Index:     entry.Index,
			Data:      entry.Data,
		})
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Index:   preLogIndex,
		LogTerm: preLogTerm,
		Entries: sendEntries,
		Commit:  r.RaftLog.committed,
	}
	r.msgs = append(r.msgs, msg)
	return true
}

func (r *Raft) sendSnapshot(to uint64) {
	snap, err := r.RaftLog.storage.Snapshot()
	if err != nil {
		return
	}
	r.msgs = append(r.msgs, pb.Message{
		MsgType:  pb.MessageType_MsgSnapshot,
		From:     r.id,
		To:       to,
		Term:     r.Term,
		Snapshot: &snap,
	})
	r.Prs[to].Next = snap.Metadata.Index + 1
}

func (r *Raft) sendAppendResponse(to uint64, reject bool) {
	m := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Reject:  reject,
		Index:   r.RaftLog.LastIndex(),
	}
	r.msgs = append(r.msgs, m)
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	commit := min(r.RaftLog.committed, r.Prs[to].Match)
	m := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Commit:  commit,
	}
	r.msgs = append(r.msgs, m)
}

func (r *Raft) compaign() {
	if r.State == StateLeader {
		return
	}
	r.becomeCandidate()
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		m := pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
			From:    r.id,
			To:      id,
			Term:    r.Term,
			Index:   r.RaftLog.LastIndex(),
			LogTerm: r.RaftLog.LastTerm(),
		}
		r.msgs = append(r.msgs, m)
	}
}

func (r *Raft) sendHeartbeatResponse(to uint64, reject bool) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Reject:  reject,
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) sendRequestVoteResponse(to uint64, reject bool) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Reject:  reject,
	}
	r.msgs = append(r.msgs, msg)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	if r.State == StateLeader {
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatTimeout = 0
			if err := r.Step(pb.Message{From: r.id, MsgType: pb.MessageType_MsgBeat}); err != nil {
				log.Debugf("sending heartbeat error: %v", err)
			}
		}
	} else {
		r.electionElapsed++
		if r.electionElapsed >= r.randomElectionTimeout {
			r.electionElapsed = 0
			if err := r.Step(pb.Message{From: r.id, MsgType: pb.MessageType_MsgHup}); err != nil {
				log.Debugf("sending start election error: %v", err)
			}
		}
	}

}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
	r.votes = nil
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.Vote = r.id
	r.Term += 1
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
	r.granted = 1
	r.rejected = 0
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)

	if r.granted > len(r.Prs)/2 {
		r.becomeLeader()
	}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.Lead = r.id
	for _, p := range r.Prs {
		p.Match = 0
		p.Next = r.RaftLog.LastIndex() + 1
	}

	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)

	//no-op
	r.appendEntry(&pb.Entry{
		EntryType: pb.EntryType_EntryNormal,
		Term:      r.Term,
		Index:     r.RaftLog.LastIndex() + 1,
		Data:      nil,
	})
	r.bcastAppend()
	r.updateCommit()
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.compaign()
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	}

	switch r.State {
	case StateFollower:
		switch m.MsgType {
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		}

	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgRequestVoteResponse:
			r.handleRequestVoteResponse(m)
		}
	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgBeat:
			for id := range r.Prs {
				if id == r.id {
					continue
				}
				r.sendHeartbeat(id)
			}
		case pb.MessageType_MsgHeartbeatResponse:
			r.handleHeartbeatResponse(m)
		case pb.MessageType_MsgPropose:
			r.handlePropose(m)
		case pb.MessageType_MsgAppendResponse:
			r.handleAppendResponse(m)
		}
	}
	return nil
}

func (r *Raft) updateCommit() {
	for i := r.RaftLog.LastIndex(); i > r.RaftLog.committed; i-- {
		cnt := 0
		for _, p := range r.Prs {
			if p.Match >= i {
				cnt++
			}
		}
		//只能提交当前任期的日志 5.4
		term, _ := r.RaftLog.Term(i)
		if cnt > len(r.Prs)/2 && term == r.Term {
			r.RaftLog.committed = i
			r.bcastAppend()
			return
		}
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).

	// (§5.1)
	if m.Term < r.Term {
		r.sendAppendResponse(m.From, true)
		return
	}

	//重置选举时间
	r.becomeFollower(m.Term, m.From)

	//日志一致性检查(§5.3)
	term, err := r.RaftLog.Term(m.Index)
	if err != nil || term != m.LogTerm {
		r.sendAppendResponse(m.From, true)
		return
	}

	//处理冲突日志
	index := m.Index
	for i, entry := range m.Entries {
		index++
		term, err := r.RaftLog.Term(index)
		if err == nil {
			if term == entry.Term {
				continue
			}
			r.RaftLog.truncateAt(index)
			//r.RaftLog.entries = r.RaftLog.entries[:index-r.RaftLog.FirstIndex()]
		}
		for _, e := range m.Entries[i:] {
			//r.RaftLog.entries = append(r.RaftLog.entries, *e)
			r.RaftLog.appendEntry(*e)
		}
		break
	}
	//set commitIndex = min(leaderCommit, index of last new entry)
	//别忘记了是new entry，如果new entry为空，则以m.Index为准
	if m.Commit > r.RaftLog.committed {
		lastNewEntry := m.Index
		if len(m.Entries) != 0 {
			lastNewEntry = m.Entries[len(m.Entries)-1].Index
		}
		r.RaftLog.committed = min(m.Commit, lastNewEntry)
	}

	//TODO  可能需要重置选举时间
	r.sendAppendResponse(m.From, false)

}

func (r *Raft) handleAppendResponse(m pb.Message) {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
		return
	}
	if m.Reject {
		r.Prs[m.From].Next--
		r.sendAppend(m.From)
		return
	}
	r.Prs[m.From].Next = m.Index + 1
	r.Prs[m.From].Match = m.Index
	r.updateCommit()
}

func (r *Raft) handlePropose(m pb.Message) {
	r.appendEntry(m.Entries...)
	r.bcastAppend()
	r.updateCommit()
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if m.Term < r.Term {
		r.sendHeartbeatResponse(m.From, true)
		return
	}
	//重设选举时间
	r.becomeFollower(m.Term, m.From)
	//更新commit
	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Commit, r.RaftLog.LastIndex())
	}
	r.sendHeartbeatResponse(m.From, false)
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
	}
	// TestCommitWithHeartbeat tests leader can send log
	// to follower when it received a heartbeat response
	// which indicate it doesn't have update-to-date log
	if r.Prs[m.From].Match < r.RaftLog.LastIndex() {
		r.sendAppend(m.From)
	}

}

func (r *Raft) handleRequestVote(m pb.Message) {
	if m.Term < r.Term {
		// 对方的任期更小，拒绝掉
		r.sendRequestVoteResponse(m.From, true)
		return
	}

	if m.Term > r.Term {
		//log.Debug("id:%v term:%v, m.Term:%v", r.id, r.Term, m.Term)
		r.becomeFollower(m.Term, None)
		//单纯任期大不一定要投票
		r.Vote = None
	}
	//要么任期更大，要么已经
	if r.Vote == None || r.Vote == m.From {
		lastTerm := r.RaftLog.LastTerm()
		lastIndex := r.RaftLog.LastIndex()
		//log.Debug("printf lastTerm:%v, lastIndex:%v, m.LogTerm:%v, m.Index%v", lastTerm, lastIndex, m.LogTerm, m.Index)
		//比较谁的日志更完整，如果对方的任期更大，或者任期相同但是日志更完整，则同意投票
		if m.LogTerm > lastTerm || (m.LogTerm == lastTerm && m.Index >= lastIndex) {
			r.sendRequestVoteResponse(m.From, false)
			r.Vote = m.From
			return
		}
		r.sendRequestVoteResponse(m.From, true)
		return
	}
	r.sendRequestVoteResponse(m.From, true)
}
func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, m.From)
		return
	}
	if !m.Reject {
		r.granted++
		r.votes[m.From] = true
	} else {
		r.rejected++
		r.votes[m.From] = false
	}
	if r.granted > len(r.Prs)/2 {
		r.becomeLeader()
	} else if r.rejected > len(r.Prs)/2 {
		r.becomeFollower(r.Term, r.Lead)
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	meta := m.Snapshot.Metadata
	if meta.Index <= r.RaftLog.committed {
		r.sendAppendResponse(m.From, false)
		return
	}

	if m.Term < r.Term {
		r.sendAppendResponse(m.From, true)
		return
	}
	r.becomeFollower(m.Term, m.From)
	r.RaftLog.entries = nil
	r.RaftLog.pendingSnapshot = m.Snapshot
	r.RaftLog.stabled = meta.Index
	r.RaftLog.applied = meta.Index
	r.RaftLog.committed = meta.Index
	r.Prs = make(map[uint64]*Progress)
	for _, p := range meta.ConfState.Nodes {
		r.Prs[p] = &Progress{}
	}
	r.sendAppendResponse(m.From, false)
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
