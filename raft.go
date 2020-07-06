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
// import "math"
// import "bytes"
// import "../labgob"



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
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
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
	DPrintf("[%d] getting request to vote from[%v], my log[%v], term[%d], commitIndex[%d]", rf.me, args, rf.log, rf.currentTerm, rf.commitIndex)
	rfTerm := rf.currentTerm
	currentState := rf.state
	if (currentState == Leader) {
		reply.Votegranted = false
		reply.Term = rf.currentTerm
	}
	if (args.Term < rfTerm) {
		reply.Votegranted = false
		reply.Term = rfTerm
	} else {
		if (args.Term > rfTerm) {
			rf.ConvertToFollower(args.Term)
		}
		finalVote := true
		for _, item := range rf.log[:rf.commitIndex] {
			if args.Lastlogindex >= item.Index && args.Lastlogterm >= item.Term {
				finalVote = true
			} else {
				finalVote = false
			}
		}
		if (finalVote) {
			// rf.ConvertToFollower(args.Term)
			reply.Votegranted = true
			reply.Term = rf.currentTerm
			DPrintf("[RequestVote-ConvertToFollower][%d] term [%d] args[%v]", rf.me, rf.currentTerm, args)
		} else {
			DPrintf("[RequestVote-ConvertToFollower][%d] candidate[%v] is not up to date[%v] in term [%d]", rf.me, args, rf.log ,rf.currentTerm)
			reply.Votegranted = false
			reply.Term = rf.currentTerm
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

// args-log entries are empty if call is just a hearbeat.
func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	// if leader's Term which comes in the RPC arg is less that the node's currentTerm then return False
	rf.mu.Lock()
	defer rf.mu.Unlock()
	currentTerm := rf.currentTerm
	// DPrintf("[AppendEntry][%d] currentTerm [%d]", rf.me, currentTerm)
	DPrintf("[AppendEntry][%d] heartbeat for term[%d] args, leaderId[%d], prevLogIndex[%d], prevLogTerm[%d], entries[%v], leaderCommit[%d], log[%v], currentTerm[%d], commitIndex[%d], lastApplied[%d]", rf.me, args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.Entries, args.LeaderCommit, rf.log, currentTerm, rf.commitIndex, rf.lastApplied)
	if (args.Term < currentTerm) {
		reply.Success = false
		reply.Term = currentTerm
	} else {
		if (args.Term > currentTerm) {
			rf.currentTerm =  args.Term
		}
		rf.ConvertToFollower(args.Term)
		if _entry := rf.getLogEntry(args.PrevLogIndex - 1); args.PrevLogIndex != 0 && _entry.Index == 0  { // same index entry different term
			DPrintf("[AppendEntry][%d] last entry[%v] is different from prevLogIndex[%v]", rf.me, rf.getLastLogEntry(), args.PrevLogIndex)
			reply.Success = false
			reply.Term = currentTerm
		} else if _entry.Index > 0 && _entry.Term != args.PrevLogTerm {
			DPrintf("[AppendEntry][%d] entry with different term entry[%v]", rf.me, _entry)
			reply.Success = false
			reply.Term = currentTerm
		} else {
			for _, item := range args.Entries {
				if existItemOnLog := rf.getLogEntry(item.Index-1); existItemOnLog.Index > 0 {
					if (existItemOnLog.Term == item.Term) {
						DPrintf("[Follower][%d] idempotent append entry send by [%d] for term[%d]", rf.me, args.LeaderId, args.Term)
						continue
					} else {
						DPrintf("[Follower][%d] conflict entry[%v] send by [%d] for term[%d]", rf.me, item, args.LeaderId, args.Term)
						DPrintf("[FOLLOWER][%d] unsync lastApplied[%d], commitIndex[%d]", rf.me, rf.lastApplied, rf.commitIndex)
						rf.log = rf.log[:existItemOnLog.Index-1]
						rf.log = append(rf.log, item)
						rf.lastApplied = rf.getLastLogEntry().Index
						DPrintf("[FOLLOWER][%d] after sync lastApplied[%d], commitIndex[%d] log[%v]", rf.me, rf.lastApplied, rf.commitIndex, rf.log)
					}

				} else {
					DPrintf("[AppendEntry][%d] append entry[%v] send by [%d] for term[%d]", rf.me, item, args.LeaderId, args.Term)
					rf.log = append(rf.log, item)
					rf.lastApplied = rf.getLastLogEntry().Index
					DPrintf("[AppendEntry][%d] 2.a lastApplied[%d] commitIndex[%d], lastLogEntry[%v]", rf.me, rf.lastApplied, rf.commitIndex, rf.getLastLogEntry())
				}

			}
			if (rf.lastApplied > 0 && args.LeaderCommit >= rf.commitIndex && rf.lastApplied != rf.commitIndex) {
				startFrom := rf.commitIndex
				if (args.LeaderCommit == rf.lastApplied) {
					for	_, entry := range rf.log[startFrom:] {
						msgTest := ApplyMsg {
							CommandValid : true,
								Command      : entry.Command,
								CommandIndex : entry.Index,
								From         : rf.me,
							}
						DPrintf("[AppendEntry][%d] msgTest[%v]", rf.me, msgTest)
						rf.applyCh <- msgTest
						rf.commitIndex = entry.Index
					}
				}
			}
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
		DPrintf("[Start][%d] leader send aggrement cmd[%v]", rf.me, command)
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
	rf.lastApplied = index
	rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		if ( i == rf.me) {
			continue
		}
		go rf.SendAppendEntry(i) // getting a successful respose from the mayority guarantees that the commmand is commited
	}
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
	// go sendEmptyAppendEntryIfLeader(rf)


	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}

func (rf *Raft) StartLeaderWork(peer int) {
	for {
		time.Sleep(10 * time.Millisecond)
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		if (rf.killed()) {
			rf.mu.Unlock()
			return
		}
		DPrintf("[StartLeaderWork][%d] to follower: [%d] for term [%d]", rf.me, peer, rf.currentTerm)
		rf.mu.Unlock()
		go rf.SendAppendEntry(peer)
		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) SendAppendEntry(peer int) {
	time.Sleep(10 * time.Millisecond)
	rf.mu.Lock()
	if rf.state != Leader {
		DPrintf("[SendAppendEntry][%d]-[%d] should not send this entry for term [%d]", rf.me, peer, rf.currentTerm)
		rf.mu.Unlock()
		return
	}
	currentTerm := rf.currentTerm
	lastLogEntry := rf.getLastLogEntry()
	var entries []LogEntry
	if (rf.lastApplied >= rf.nextIndex[peer]) {
		if (rf.nextIndex[peer] > 0) {
			startFrom := rf.nextIndex[peer] - 1
			entries = rf.log[startFrom:]
		}
	}
	if len(entries) > 0 {
		lastEntryIndex := entries[0].Index
		lastLogEntry = rf.getLogEntry(lastEntryIndex - 2)
	}
	// DPrintf("[SendAppendEntry][%d]-[%d] lastApplied[%d], nextIndex [%v] in term [%d] entries[%v] lastLogEntry[%v], lastIndex[%d]", rf.me, peer, rf.lastApplied, rf.nextIndex[peer], currentTerm, entries, lastLogEntry, lastIndex)
	aeargs := &AppendEntryArgs{
		Term: currentTerm,
		LeaderId: rf.me,
		PrevLogIndex: lastLogEntry.Index,
		PrevLogTerm: lastLogEntry.Term,
		Entries: entries,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()
	aereply := &AppendEntryReply{}
	rf.sendAppendEntry(peer, aeargs, aereply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if aereply.Success && rf.state == Leader {
		if (len(entries) > 0) {
			lastSendEntry := entries[len(entries)-1]
			if rf.matchIndex[peer] < lastSendEntry.Index {
				rf.nextIndex[peer] = lastSendEntry.Index + 1
				rf.matchIndex[peer] = lastSendEntry.Index
				DPrintf("[SendAppendEntry][%d] Success[%d], nextIndex [%v], matchIndex[%v] in term [%d]", rf.me, peer, rf.nextIndex[peer], rf.matchIndex[peer], rf.getLastLogEntry().Term)
			}
		}

		DPrintf("[SendAppendEntry][%d] after success lastApplied[%d], commitIndex[%d], lastEntryTerm[%d], currentTerm[%d] matchIndex[%v]", rf.me, rf.lastApplied, rf.commitIndex, rf.getLastLogEntry().Term, rf.currentTerm, rf.matchIndex)
		if (rf.lastApplied > rf.commitIndex) {
			majorityMatch := 1
			for _, value := range rf.matchIndex {
				if value >= rf.lastApplied {
					majorityMatch++
				}
				if majorityMatch > len(rf.peers)/2 {
					startCommitIndex := rf.commitIndex
					for	_, entry := range rf.log[startCommitIndex:] {
						msgTest := ApplyMsg {
							CommandValid : true,
								Command      : entry.Command,
								CommandIndex : entry.Index,
								From         : rf.me,
							}
						DPrintf("[Leader][%d] msgTest[%v]", rf.me, msgTest)
						rf.applyCh <- msgTest
						rf.commitIndex = entry.Index
					}
					DPrintf("[CommitIndexProcess][%d] cmd replicated lastApplied[%d] matchIndex[%v], log[%v], new commit index[%d]", rf.me, rf.lastApplied, rf.matchIndex, rf.log, rf.commitIndex)
					break
				}
			}
		}
	} else {
		if (aereply.Term > rf.currentTerm) {
			DPrintf("[Leader-ConverToFollower][%d] term [%d] peer[%d]", rf.me, aereply.Term, peer)
			rf.ConvertToFollower(aereply.Term)
		} else {
			if rf.nextIndex[peer] >= 1 && aereply.Term !=0 && rf.state == Leader {
				DPrintf("[SendAppendEntry][%d] decrease nextIndex of [%d]", rf.me, peer)
				rf.nextIndex[peer] -= 1
			}
		}
	}
}

// func (rf *Raft) HandlerAppendEntry(rvreply *AppendEntryReply, entries []LogEntry) bool {
// }

func (rf *Raft) AttemptElection() {
	numVote := 1
	for i := 0; i < len(rf.peers); i++ {
		if ( i == rf.me) {
			continue
		}
		go func(peer int) {
			rvreply := &RequestVoteReply{}
			a := rf.CallRequestVote(peer, rvreply)
			if !a {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			DPrintf("[AttemptElection][%d] rvreply[%v]", rf.me, rvreply)
			if rvreply.Term > rf.currentTerm {
				DPrintf("[AttemptElection-ConvertToFollower][%d] term [%d] peer[%d]", rf.me, rvreply.Term, peer)
				rf.ConvertToFollower(rvreply.Term)
				return
			}
			if rvreply.Votegranted {
				numVote++
				if numVote > len(rf.peers)/2 && rf.state == Candidate {
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
			return
		}(i)
	}
}

func (rf *Raft) CallRequestVote(peer int, reply *RequestVoteReply) bool {
	rf.mu.Lock()
	lastLogEntry := rf.getLastLogEntry()
	rvargs := &RequestVoteArgs{
		Term: rf.currentTerm,
		Candidateid: rf.me,
		Lastlogindex: lastLogEntry.Index,
		Lastlogterm: lastLogEntry.Term,
	}
	rf.mu.Unlock()
	positiveVote := rf.sendRequestVote(peer, rvargs, reply)
	return positiveVote
}

func (rf *Raft) ConvertToFollower(newTerm int) {
	rf.state = Follower
	rf.currentTerm = newTerm
	rf.votedFor = -1
	rf.lastHearBeat = time.Now()
}

func (rf *Raft) ConvertToCandidate() {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.lastHearBeat = time.Now()

}

func (rf *Raft) ConvertToLeader() {
	lastLogEntry := rf.getLastLogEntry()
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
	rf.lastHearBeat = time.Now()
}

func startRequestVoteProcessIfNeeded(rf *Raft) {
	// at start every node has a hearbeat in zero
	for {
		time.Sleep(10 * time.Millisecond)
		if (rf.killed()) {
			break
		}
		rand.Seed(time.Now().UnixNano())
		randomSleep := (rand.Int63n(200) + 200)
		randomWait := time.Duration(randomSleep) * time.Millisecond
		time.Sleep(randomWait)
		if (rf.killed()) {
			break
		}
		rf.mu.Lock()
		currentHearBeat := rf.lastHearBeat
		currentState  := rf.state
		currentTime := time.Now()
		diffBeat := currentTime.Sub(currentHearBeat)
		DPrintf("[startRequestVoteProcessIfNeeded][%d]", rf.me)
		rf.mu.Unlock()
		if (diffBeat.Milliseconds() > randomWait.Milliseconds()) {
			if currentState != Leader {
				rf.mu.Lock()
				rf.ConvertToCandidate()
				DPrintf("[AttemptElection][%d] for term[%d]", rf.me, rf.currentTerm)
				rf.mu.Unlock()
				rf.AttemptElection()
			}
		}
	}
	DPrintf("[startRequestVoteProcessIfNeeded] end node[%d]", rf.me)
}
