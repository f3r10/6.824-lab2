package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import "sync/atomic"
import "../labrpc"
import "time"
import "math/rand"
import "math"
import "bytes"
import "../labgob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	From int
}

//
// A Go object implementing a single Raft peer.
//
type LogEntry struct {
	Index int
	Term int
	Command interface{}

}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh chan ApplyMsg

	// not defined in figure 2 from the paper
	state State // Candidate, Follower or Leader
	lastHearBeat time.Time

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor int
	log []LogEntry

	commitIndex int // once	every other follower replies a successful response, the leader would set the last added entry as the commited index
	lastApplied int


	nextIndex map[int]int
	matchIndex map[int]int

}

type State string

const (
	Leader State = "leader"
	Follower State = "follower"
	Candidate State = "candidate"
)

func (rf *Raft) getLastLogEntry() (LogEntry) {
	if len(rf.log) > 0 {
		return rf.log[len(rf.log)-1]
	} else {
		return LogEntry{}
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.log)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	rf.mu.Lock()
	if data == nil || len(data) < 1 { // bootstrap without any state?
		rf.mu.Unlock()
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var log []LogEntry
	var currentTerm int
	var votedFor int
	if d.Decode(&log) != nil ||
		d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil {
		panic("gob: attempt to decode into a non-pointer")
	} else {
		rf.log = log
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
	}
	DPrintf("[%d] read persist data votedFor[%d], currentTerm[%d], lastLogEntry[%v]", rf.me, rf.votedFor, rf.currentTerm, rf.getLastLogEntry())
	rf.mu.Unlock()
}



type AppendEntryArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommit int
}

type AppendEntryReply struct {
	Term int
	Success bool
	// for roll back quickly
	XTerm int
	XIndex int
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	Candidateid int
	Lastlogindex int
	Lastlogterm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	Votegranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[%d] getting request to vote from[%v], last log entry[%v], term[%d], commitIndex[%d]", rf.me, args, rf.getLastLogEntry(), rf.currentTerm, rf.commitIndex)
	currentState := rf.state
	if (currentState == Leader) {
		rf.persist()
		reply.Votegranted = false
		reply.Term = rf.currentTerm
		return
	} else {
		if (args.Term < rf.currentTerm) {
			rf.persist()
			reply.Votegranted = false
			reply.Term = rf.currentTerm
			return
		} else {
			if (args.Term > rf.currentTerm) {
				rf.ConvertToFollower(args.Term)
			}
			if (rf.votedFor == -1 || rf.votedFor == args.Candidateid) {
				if entry := rf.getLastLogEntry(); entry.Index == 0 {
					rf.ConvertToFollower(args.Term)
					rf.votedFor = args.Candidateid
					rf.ResetTimer()
					rf.persist()
					DPrintf("[RequestVote-ConcedeVote-1][%d] term [%d] args[%v] entry[%v]", rf.me, rf.currentTerm, args, entry)
					reply.Votegranted = true
					reply.Term = rf.currentTerm
					return
				} else if args.Lastlogterm >= entry.Term {
					if entry.Term == args.Lastlogterm && entry.Index > args.Lastlogindex {
						DPrintf("[RequestVote-DenyVote-3][%d] candidate[%v] is not up to date in term [%d]", rf.me, args,rf.currentTerm)
						rf.persist()
						reply.Votegranted = false
						reply.Term = rf.currentTerm
						return
					} else {
						rf.ConvertToFollower(args.Term)
						rf.votedFor = args.Candidateid
						rf.ResetTimer()
						rf.persist()
						DPrintf("[RequestVote-ConcedeVote-2][%d] term [%d] args[%v] entry[%v]", rf.me, rf.currentTerm, args, entry)
						reply.Votegranted = true
						reply.Term = rf.currentTerm
						return
					}
				} else {
						DPrintf("[RequestVote-DenyVote-4][%d] candidate[%v] is not up to date in term [%d]", rf.me, args, rf.currentTerm)
						rf.persist()
						reply.Votegranted = false
						reply.Term = rf.currentTerm
						return
					}
			} else {
				DPrintf("[RequestVote][%d] already voted for [%d]", rf.me, rf.votedFor)
				rf.persist()
				reply.Votegranted = false
				reply.Term = rf.currentTerm
				return
			}
		}
	}
}

func (rf *Raft) getLogEntry(position int) LogEntry {
	if (position >= 0 && position < len(rf.log)) {
		return rf.log[position]
	} else {
		return LogEntry{}
	}
}
func (rf *Raft) CheckLogEntryAt(prevLogIndex int, prevLogTerm int) bool {
	if (len(rf.log) == 0) {
		return false

	} else {
		if (prevLogIndex <= len(rf.log)) {
			entryLogAt := rf.log[prevLogIndex]
			return entryLogAt.Term != prevLogTerm
		} else {
			return true
		}
	}
}

func (rf *Raft) GetLastEntryWithTerm(term int, from int) int {
	default_entry := 0
	for i := from; i >= 0; i-- {
		if (rf.log[i].Term != term) {
			return i
		} else {
			continue
		}
	}
	return default_entry
}

// args-log entries are empty if call is just a hearbeat.
func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	// if leader's Term which comes in the RPC arg is less that the node's currentTerm then return False
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.ResetTimer()
	currentTerm := rf.currentTerm
	DPrintf("[AppendEntry][%d] heartbeat for term[%d] args, leaderId[%d], prevLogIndex[%d], prevLogTerm[%d], entries[%v], leaderCommit[%d], currentTerm[%d], commitIndex[%d], lastApplied[%d]", rf.me, args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.Entries, args.LeaderCommit, currentTerm, rf.commitIndex, rf.lastApplied)
	if (args.Term < currentTerm) {
		rf.persist()
		reply.Success = false
		reply.Term = currentTerm
		return
	} else {
		if _entry := rf.getLogEntry(args.PrevLogIndex - 1); args.PrevLogIndex != 0 && _entry.Index == 0  {
			DPrintf("[AppendEntry][%d] last entry[%v] is different from prevLogIndex[%v]", rf.me, rf.getLastLogEntry(), args.PrevLogIndex)
			rf.persist()
			reply.Success = false
			reply.Term = currentTerm
			reply.XTerm = -1
			reply.XIndex = len(rf.log)
			return
		} else if _entry.Index > 0 && _entry.Term != args.PrevLogTerm {
			DPrintf("[AppendEntry][%d] entry with different term entry[%v]", rf.me, _entry)
			rf.persist()
			reply.Success = false
			reply.Term = currentTerm
			reply.XTerm = _entry.Term
			reply.XIndex = rf.GetLastEntryWithTerm(_entry.Term, args.PrevLogIndex - 1)
			return
		} else {
			if (args.Term > currentTerm) {
				rf.ConvertToFollower(args.Term)
			}
			for _, item := range args.Entries {
				if existItemOnLog := rf.getLogEntry(item.Index-1); existItemOnLog.Index > 0 {
					if (existItemOnLog.Term == item.Term) {
						// DPrintf("[Follower][%d] idempotent append entry send by [%d] for term[%d]", rf.me, args.LeaderId, args.Term)
						continue
					} else {
						DPrintf("[Follower][%d] conflict entry[%v] send by [%d] for term[%d]", rf.me, item, args.LeaderId, args.Term)
						DPrintf("[FOLLOWER][%d] unsync lastApplied[%d], commitIndex[%d]", rf.me, rf.lastApplied, rf.commitIndex)
						rf.log = rf.log[:existItemOnLog.Index-1]
						rf.log = append(rf.log, item)
						DPrintf("[FOLLOWER][%d] after sync lastApplied[%d], commitIndex[%d]", rf.me, rf.lastApplied, rf.commitIndex)
					}

				} else {
					// DPrintf("[AppendEntry][%d] append entry[%v] send by [%d] for term[%d]", rf.me, item, args.LeaderId, args.Term)
					rf.log = append(rf.log, item)
				}

			}
			DPrintf("[AppendEntry====1][%d] LeaderCommit[%d], commitIndex[%d], indexLastLongEntry[%d]", rf.me, args.LeaderCommit, rf.commitIndex, rf.getLastLogEntry().Index)
			if (args.LeaderCommit > rf.commitIndex) {
				rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(rf.getLastLogEntry().Index)))
			}

			DPrintf("[AppendEntry====2][%d] LastApplied[%d], commitIndex[%d]", rf.me, rf.lastApplied, rf.commitIndex)
			for rf.lastApplied < rf.commitIndex {
				entryToCommit := rf.log[rf.lastApplied]
				msgTest := ApplyMsg {
					CommandValid : true,
						Command      : entryToCommit.Command,
						CommandIndex : entryToCommit.Index,
						From         : rf.me,
					}
				DPrintf("[AppendEntry-Follower][%d] msgTest[%v]", rf.me, msgTest)
				rf.applyCh <- msgTest
				rf.lastApplied++
			}
			rf.persist()
			reply.Success = true
			reply.Term = currentTerm
			return
		}
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.peers[server].Call("Raft.AppendEntry", args, reply)
}
//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	term, isLeader = rf.GetState()

	// Your code here (2B).
	if !isLeader {
		return index, term, isLeader
	} else {
		return rf.ReplicateLogEntry(command, term)
	}
}

func (rf *Raft) ReplicateLogEntry(command interface{}, term int) (int, int, bool) {
	// agreement should start because it is a leader node
	// I think that agreement means append the entry command to the shared log.
	// This means of course fire up the RPCAppendEtnry
	// if SendAppendEntry is successful, the leader should append the entry to its log entries?
	rf.mu.Lock()
	index := rf.getLastLogEntry().Index + 1
	entry := LogEntry {
		Index: index,
			Term: term,
			Command: command,
		}

	rf.log = append(rf.log, entry)
	DPrintf("[Start][%d] client send cmd[%v] for index[%d]", rf.me, command, index)
	rf.mu.Unlock()
	return index, term, true
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.lastHearBeat = time.Now()
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	DPrintf("[make] %d initialized", rf.me)
	go startRequestVoteProcessIfNeeded(rf)


	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	return rf
}

func (rf *Raft) StartLeaderWork(peer int) {
	for {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		if (rf.killed()) {
			rf.mu.Unlock()
			return
		}
		DPrintf("[StartLeaderWork-before][%d] to follower: [%d] for term [%d]", rf.me, peer, rf.currentTerm)
		rf.persist();
		rf.mu.Unlock()
		go rf.SendAppendEntry(peer)
		time.Sleep(150 * time.Millisecond)
	}
}

func (rf *Raft) SendAppendEntry(peer int) {
	rf.mu.Lock()
	if rf.state != Leader || rf.killed() {
		DPrintf("[SendAppendEntry][%d]-[%d] not leader or alive anymore for term [%d]", rf.me, peer, rf.currentTerm)
		rf.mu.Unlock()
		return
	}
	DPrintf("[SendAppendEntry-after][%d]-[%d] lastApplied[%d], nextIndex [%v] in term [%d] log len[%d]", rf.me, peer, rf.lastApplied, rf.nextIndex[peer], rf.currentTerm, len(rf.log))
	prevLogIndex := rf.nextIndex[peer] - 1
	var entries []LogEntry
	var prevLogTerm int
	if e := rf.getLogEntry(prevLogIndex-1); e.Index > 0 {
		prevLogTerm = e.Term
		entries = rf.log[rf.nextIndex[peer]-1:]
	} else {
		entries = rf.log[:]
	}
	aeargs := &AppendEntryArgs{
		Term: rf.currentTerm,
		LeaderId: rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm: prevLogTerm,
		Entries: entries,
		LeaderCommit: rf.commitIndex,
	}
	aereply := &AppendEntryReply{}
	rf.mu.Unlock()
	rf.sendAppendEntry(peer, aeargs, aereply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if aereply.Success && rf.state == Leader {
		DPrintf("[SendAppendEntry][%d] Success[%d], nextIndex [%v], matchIndex[%v] in term [%d] entries [%v]", rf.me, peer, rf.nextIndex[peer], rf.matchIndex[peer], rf.getLastLogEntry().Term, entries)
		if (len(aeargs.Entries) > 0) {
			lastSendEntry := aeargs.Entries[len(aeargs.Entries)-1]
			if rf.matchIndex[peer] < lastSendEntry.Index {
				rf.nextIndex[peer] = lastSendEntry.Index + 1
				rf.matchIndex[peer] = lastSendEntry.Index
				DPrintf("[SendAppendEntry][%d] Success[%d], nextIndex [%v], matchIndex[%v] in term [%d]", rf.me, peer, rf.nextIndex[peer], rf.matchIndex[peer], rf.getLastLogEntry().Term)
			}
		}

		DPrintf("[SendAppendEntry][%d] after success lastApplied[%d], commitIndex[%d], lastEntryTerm[%d], currentTerm[%d] matchIndex[%v]", rf.me, rf.lastApplied, rf.commitIndex, rf.getLastLogEntry().Term, rf.currentTerm, rf.matchIndex)
		majorityMatch := len(rf.peers)/2
		for index := rf.commitIndex + 1; index <= rf.getLastLogEntry().Index; index++ {
			count := 1
			for server, match := range rf.matchIndex {
				if server != rf.me && match >= index {
					count++
				}
			}

			if count > majorityMatch && rf.getLogEntry(index-1).Term == rf.currentTerm {
				rf.commitIndex = index
			}

		}
		for rf.lastApplied < rf.commitIndex {
			entryToCommit := rf.log[rf.lastApplied]
			msgTest := ApplyMsg {
				CommandValid : true,
					Command      : entryToCommit.Command,
					CommandIndex : entryToCommit.Index,
					From         : rf.me,
				}
			DPrintf("[Leader][%d] msgTest[%v]", rf.me, msgTest)
			rf.applyCh <- msgTest
			rf.lastApplied++
		}
		DPrintf("[CommitIndexProcess][%d] cmd replicated lastApplied[%d] matchIndex[%v], new commit index[%d]", rf.me, rf.lastApplied, rf.matchIndex, rf.commitIndex)
	} else {
		if (aereply.Term > rf.currentTerm) {
			DPrintf("[Leader-ConverToFollower][%d] term [%d] peer[%d]", rf.me, aereply.Term, peer)
			rf.ConvertToFollower(aereply.Term)
			return
		}

		DPrintf("[SendAppendEntry][%d] decrease nextIndex of [%d], reply[%v]", rf.me, peer, aereply)
		if (aereply.XTerm == -1) {
			rf.nextIndex[peer] = aereply.XIndex + 1
			return
		} else {
			findConflictedTerm := false
			for i := len(rf.log)-1; i >= 0; i-- {
				if (rf.log[i].Term == aereply.XTerm) {
					findConflictedTerm = true
					rf.nextIndex[peer] = rf.log[i].Index + 1
					break
				}
			}
			if (!findConflictedTerm) {
				rf.nextIndex[peer] = aereply.XIndex + 1
			}
			return
		}
	}
}

func (rf *Raft) AttemptElection() {
	numVote := 1
	for i := 0; i < len(rf.peers); i++ {
		if ( i == rf.me) {
			continue
		}
		go func(peer int) {
			rf.mu.Lock()
			lastLogEntry := rf.getLastLogEntry()
			rvargs := &RequestVoteArgs{
				Term: rf.currentTerm,
				Candidateid: rf.me,
				Lastlogindex: lastLogEntry.Index,
				Lastlogterm: lastLogEntry.Term,
			}
			rf.persist();
			rvreply := &RequestVoteReply{}
			rf.mu.Unlock()
			positiveVote := rf.sendRequestVote(peer, rvargs, rvreply)
			if !positiveVote {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			DPrintf("[AttemptElection][%d] rvreply[%v] from [%d] currentTerm[%d]", rf.me, rvreply, peer, rf.currentTerm)
			if rvreply.Term > rf.currentTerm {
				DPrintf("[AttemptElection-ConvertToFollower][%d] term [%d] peer[%d]", rf.me, rvreply.Term, peer)
				rf.ConvertToFollower(rvreply.Term)
				return
			}
			if rvreply.Votegranted {
				numVote++
				if numVote > len(rf.peers)/2 && rf.state == Candidate && rvargs.Term == rf.currentTerm {
					rf.ConvertToLeader()
					DPrintf("[AttemptElection][%d] leader for term [%d]", rf.me, rf.currentTerm)
					for i := 0; i < len(rf.peers); i++ {
						if ( i == rf.me) {
							continue
						}
						go rf.StartLeaderWork(i)
					}
				}
			}
		}(i)
	}
}

func (rf *Raft) ConvertToFollower(newTerm int) {
	rf.state = Follower
	rf.currentTerm = newTerm
	rf.votedFor = -1
}

func (rf *Raft) ResetTimer() {
	rf.lastHearBeat = time.Now()
}

func (rf *Raft) ConvertToCandidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.lastHearBeat = time.Now()
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me

}

func (rf *Raft) ConvertToLeader() {
	lastLogEntry := rf.getLastLogEntry()
	rf.lastHearBeat = time.Now()
	rf.state = Leader
	rf.nextIndex = make(map[int]int)
	rf.matchIndex = make(map[int]int)
	for i := 0; i < len(rf.peers); i++ {
		if ( i == rf.me) {
			continue
		}
		rf.nextIndex[i] = ( lastLogEntry.Index + 1 )
		rf.matchIndex[i] = 0
	}
}

func startRequestVoteProcessIfNeeded(rf *Raft) {
	// at start every node has a hearbeat in zero
	for {
		time.Sleep(10 * time.Millisecond)
		if (rf.killed()) {
			break
		}
		rand.Seed(time.Now().UnixNano())
		randomSleep := (rand.Int63n(200) + 250)
		randomWait := time.Duration(randomSleep) * time.Millisecond
		time.Sleep(randomWait)
		if (rf.killed()) {
			break
		}
		rf.mu.Lock()
		startElection := (time.Now().Sub(rf.lastHearBeat).Milliseconds() > randomWait.Milliseconds()) && rf.state != Leader
		rf.mu.Unlock()
		DPrintf("[startRequestVoteProcessIfNeeded][%d]", rf.me)
		if (startElection) {
			rf.ConvertToCandidate()
			DPrintf("[AttemptElection][%d] for term[%d]", rf.me, rf.currentTerm)
			rf.AttemptElection()
		}
	}
	DPrintf("[startRequestVoteProcessIfNeeded] end node[%d]", rf.me)
}
