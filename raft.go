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

import (
	"cs350/labrpc"
	"fmt"
	"log"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// https://marcofranssen.nl/how-to-do-enums-in-go
type serverState string

const (
	Leader    serverState = "Leader"
	Follower  serverState = "Follower"
	Candidate serverState = "Candidate"
)

// import "bytes"
// import "cs350/labgob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// for 2A
	State serverState // starts as follower
	//Persistent state on all servers:
	// (Updated on stable storage before responding to RPCs)
	CurrentTerm int        // the current term
	VotedFor    int        //candidateId that received vote in current term (or null if none)
	Log         []LogEntry // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	//Volatile state on all servers:
	CommitIndex int //index of highest log entry known to be committed (initialized to 0, increases monotonically)
	LastApplied int //index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	//Volatile state on leaders:
	//(Reinitialized after election)

	NextIndex    []int     // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	MatchIndex   []int     //for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically
	ElectionTime time.Time // election time

	CommitSignal chan bool // signal to start commit and apply msg to client
}

// Go object for log entry
type LogEntry struct {
	Index      int
	EntryTerm  int         // the current term of the learder that stored command
	LogCommand interface{} // command from the user/ client ?? what data type???
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // the current term of the requesting server(candidate)
	CandidateId  int // id for each candidate
	LastLogIndex int // the last log index in cadidates' log
	LastLogTerm  int //term of candidate’s last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).

	Term        int  //current term, for the candiate to update itself
	VoteGranted bool //true means candidate received vote

}

type AppendEntriesArgs struct {
	LeaderId     int        //so follower can redirect clients
	LeaderTerm   int        //leader’s term
	PrevLogIndex int        //index of log entry immediately preceding new ones
	PrevLogTerm  int        //term of prevLogIndex entry
	Entries      []LogEntry //log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        //leader’s commitIndex

}

type AppendEntriesReply struct {
	Term    int  //currentTerm, for leader to update itself
	Success bool //true if follower contained entry matching prevLogIndex and prevLogTerm

}

func NewElectionTime() time.Time {
	// needs to be larger than 150ms-300ms interval that is mentioned in the paper
	// from piazza post @464
	min := 900  //min election time out
	max := 1800 // max election time out
	return time.Now().Add(time.Duration(rand.Intn(max-min)+min) * time.Millisecond)
}

// step down and become follower
// todo
func (rf *Raft) stepDown(term int) {
	rf.State = Follower
	rf.VotedFor = -1 // should this be turned to candidateid ?
	rf.ElectionTime = NewElectionTime()
	rf.CurrentTerm = term
	// nil for the followers, because they are not the leader and do not need this
	rf.NextIndex = nil
	rf.MatchIndex = nil
	//log.Println("151 rf Term ", rf.CurrentTerm, "id", rf.me, "step down to", rf.State, " votedfor", rf.VotedFor)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	//var term int
	//var isleader bool
	// Your code here (2A).
	//term = rf.currentTerm
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.CurrentTerm, rf.State == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
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

// restore previously persisted state.
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

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

func (rf *Raft) IfCommit(term int) {
	// helper function to check if there is a need to commit the new log entry
	// If there exists an N such that N > commitIndex, a majority
	// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
	// set commitIndex = N (§5.3, §5.4).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.State != Leader || rf.CurrentTerm != term {
		// make sure the server calling this function is the leader
		return
	}

	lastLogIndex := rf.Log[len(rf.Log)-1].Index

	//leader commit index == lastlog index then the log is already commited
	// to not waste time commiting logs that are already commited
	if rf.CommitIndex == lastLogIndex {
		return
	}

	for N := rf.CommitIndex + 1; N < len(rf.Log); N++ {
		if rf.Log[N].EntryTerm == term {
			count := 1
			for i := range rf.peers {
				if i != rf.me && rf.MatchIndex[i] >= N {
					count++
				}
			}
			if count >= len(rf.peers)/2+1 {
				rf.CommitIndex = N
			}
		}
	}
	go func() { rf.CommitSignal <- true }() // signal to let the commiter know that it can commit

}

// compare commit index to last applied
// increment last applied until commit index
// respond to client each time a log is committed
func (rf *Raft) Commiter(signal <-chan bool, applyCh chan<- ApplyMsg) {
	// check if killed
	for !rf.killed() {
		//wait for signal
		<-signal

		rf.mu.Lock()
		// try to commit until the current commit index, before the leader is taken out/dies
		for rf.CommitIndex > rf.LastApplied {
			//If commitIndex > lastApplied: increment lastApplied, apply
			//log[lastApplied] to state machine (§5.3)
			rf.LastApplied++
			logEntry := rf.Log[rf.LastApplied]

			// unlock because there is no change to the state
			rf.mu.Unlock()

			applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      logEntry.LogCommand,
				CommandIndex: logEntry.Index,
			}
			// lock for the next iteration
			rf.mu.Lock()
		}
		//unlock to end the entire process
		rf.mu.Unlock()
	}
}

// appendEntries rpc
// TODO
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// not sure if appendentries need a lock
	rf.mu.Lock()
	defer rf.mu.Unlock()

	log.Println("AppendEntries Server(Leader)", args.LeaderId, "to server", rf.me, "args.PrevLogIndex", args.PrevLogIndex, "rf.Log(len)", len(rf.Log))
	reply.Term = rf.CurrentTerm
	reply.Success = false

	if args.LeaderTerm < rf.CurrentTerm {
		//1. Reply false if term < currentTerm (§5.1)
		log.Println("1.Append Entries term", args.LeaderTerm, "server(leader)", args.LeaderId, "rf term", rf.CurrentTerm, "id", rf.me)

		return
	}
	rf.ElectionTime = NewElectionTime()
	//2. reply false if log doesn't contain an entry at prevlogindex whose term matches prevlogte
	if len(rf.Log) <= args.PrevLogIndex || rf.Log[args.PrevLogIndex].EntryTerm != args.PrevLogTerm {
		log.Println("2.Append Entries ")
		return
	}

	// step down
	if args.LeaderTerm > rf.CurrentTerm {
		// Rules for servers
		//If RPC request or response contains term T > currentTerm:
		//set currentTerm = T, convert to follower (§5.1)
		log.Println("AppendEntr Server(leader)", args.LeaderId, "to server", rf.me, "stepdown 1")
		rf.stepDown(args.LeaderTerm)
	}
	if rf.State == Candidate && args.LeaderTerm >= rf.CurrentTerm {
		log.Println("AppendEntr Server(leader)", args.LeaderId, "to server", rf.me, "stepdown 2")
		rf.stepDown(args.LeaderTerm)
	}

	//Since the 2. requirement already checks if the prevlog is or is not up to date and returns to the rpc caller
	// the prevlog index and term from the args would be updated to the matching one.

	if len(args.Entries) > 0 {
		//3. if an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
		log.Println("3. Append Entry existing entry conflict ")
		currentEntries := rf.Log[args.PrevLogIndex+1:]
		var idx int
		for idx = 0; idx < int(math.Min(float64(len(currentEntries)), float64(len(args.Entries)))); idx++ {
			if currentEntries[idx].EntryTerm != args.Entries[idx].EntryTerm {
				// delete the none matching log, and block
				rf.Log = rf.Log[:args.PrevLogIndex+1+idx]
				break
			}
		}
		if idx < len(args.Entries) {
			log.Println("4. append any new etries not already in the log")
			// 4. append any new entries not already in the log
			rf.Log = append(rf.Log, args.Entries[idx:]...)
		}

	}

	if args.LeaderCommit > rf.CommitIndex {
		//5. if leaderCommit > commitIndex, set CommitIndex= min(LeaderCommit, index of last new entry)
		log.Println("5. AppendEntries update commit index and send commit signal")
		rf.CommitIndex = int(math.Min(float64(args.LeaderCommit), float64(rf.Log[len(rf.Log)-1].Index)))

		// need a way to commit and let the leader know
		go func() { rf.CommitSignal <- true }()

	}
	reply.Success = true
	log.Println("End AppendEntry Server(leader)", args.LeaderId, "to server", rf.me, "reply suc", reply.Success, "reply term", reply.Term)

}

func (rf *Raft) sentAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	log.Println("sentApE server(leader)", rf.me, "to server", server, "args", fmt.Sprintf("%+v", args), "reply", reply)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	log.Println("sentApE after server(leader)", rf.me, "to server", server, "reply", reply)
	return ok
}

// using this function to construct the appendEntrie arguments because it is used multiple times in the
// ticker function, either for heartbeat or normal appendEntries
// func (rf *Raft) makeArgsApEt(server int) *AppendEntriesArgs {
// 	rf.mu.Lock()
// 	log.Println("MakeArgsApEt server(leader)", rf.me, "to server", server)
// 	// get the prevLogIndex through nextIndex -1
// 	//get the prevLogTerm trough the prev logindex's term
// 	// this would mean that the most recent entry is the prelog
// 	prevLogIndex := rf.NextIndex[server] - 1
// 	log.Println("rf.NextIndex[server]", rf.NextIndex[server], "server", server,"server(leader)",rf.me )
// 	log.Println("prevLogIndex in makeArgsApEt", prevLogIndex)
// 	//log.Println("the log", rf.Log)
// 	prevLogTerm := rf.Log[prevLogIndex].EntryTerm

// 	//checking if there is a need for Entries
// 	// then get the log entries from the point where both logs agree
// 	var entries []LogEntry
// 	if rf.Log[len(rf.Log)-1].Index >= rf.NextIndex[server] {
// 		//slicing from the matching point to the end of the Log to construct the needed entries
// 		entries = rf.Log[rf.NextIndex[server]:]
// 	}

// 	rf.mu.Unlock()
// 	// formatting and return
// 	return &AppendEntriesArgs{
// 		LeaderId:     rf.me,
// 		LeaderTerm:   rf.CurrentTerm,
// 		PrevLogIndex: prevLogIndex,
// 		PrevLogTerm:  prevLogTerm,
// 		Entries:      entries,
// 		LeaderCommit: rf.CommitIndex,
// 	}

// }

// helper function that deals with sending appendentries and dealing with the reply
// more for shrinking the ticker function and not doing everything there
func (rf *Raft) helpApEtReplySend(server int, term int) {
	rf.mu.Lock()
	//check if the server rf is still the leader
	if rf.State != Leader || rf.CurrentTerm != term {
		// if not, then unlock and return, so the ticker could continue
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock() // unlock, no read or write
	//log.Println("helAp server(leader)", rf.me, "to server", server, "make argsApEt")
	// construct the argument for append entry

	rf.mu.Lock()
	log.Println("MakeArgsApEt server(leader)", rf.me, "to server", server, "rf.NexIndx(server)", rf.NextIndex[server])
	// get the prevLogIndex through nextIndex -1
	//get the prevLogTerm trough the prev logindex's term
	// this would mean that the most recent entry is the prelog
	log.Println("Server index:", server, "Servers len:", len(rf.peers))
	prevLogIndex := rf.NextIndex[server] - 1 // The problem should be here
	log.Println("rf.NextIndex[server]", rf.NextIndex[server], "server", server)
	log.Println("prevLogIndex in makeArgsApEt", prevLogIndex, "server", server, "server(leader)", rf.me)
	//log.Println("the log", rf.Log)
	prevLogTerm := rf.Log[prevLogIndex].EntryTerm

	//checking if there is a need for Entries
	// then get the log entries from the point where both logs agree
	var entries []LogEntry
	if rf.Log[len(rf.Log)-1].Index >= rf.NextIndex[server] {
		//slicing from the matching point to the end of the Log to construct the needed entries
		entries = rf.Log[rf.NextIndex[server]:]
	}

	args := AppendEntriesArgs{
		LeaderId:     rf.me,
		LeaderTerm:   rf.CurrentTerm,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.CommitIndex,
	}
	rf.mu.Unlock()

	reply := AppendEntriesReply{} // empty reply
	log.Println("helpAp Server(leader)", rf.me, "to server", server, "prevLogIndex", args.PrevLogIndex)
	response := rf.sentAppendEntries(server, &args, &reply) //send the append entries
	log.Println("helpAp Server(leader)", rf.me, "to server", server, "response for appendentry", response, "reply.Success", reply.Success, "reply.Term", reply.Term)

	if response {
		rf.mu.Lock()
		// checking is the term was changed, when the receiving server has a higher term than the current rf
		if reply.Term > rf.CurrentTerm {
			rf.stepDown(reply.Term)
		}

		if rf.State == Leader {
			// continue if still the leader
			if reply.Success {
				// update the next and match index
				rf.NextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
				rf.MatchIndex[server] = args.PrevLogIndex + len(args.Entries)

				go rf.IfCommit(term) // check if needs to ommit
			} else {
				// when the reply.success if false, the first two conidtions
				// how to use the reply term, then move to the first index of the term?
				rf.NextIndex[server]-- // or I could rf.NextIndex[server]=1 to give the entire log to the follower
				if rf.NextIndex[server] < 1 {
					rf.NextIndex[server] = 1
				}
				go rf.helpApEtReplySend(server, term)
			}
		}
		rf.mu.Unlock()
	}

}

// example RequestVote RPC handler.
// TODO
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	//defer rf.mu.Unlock()

	//log.Println("280 Term args ", args.Term, " for server ", args.CandidateId, "asking server ", rf.me)

	reply.Term = rf.CurrentTerm
	reply.VoteGranted = false

	//reply false if term< currentTerm
	// also make sure the one sending the requestvote is not the current server
	if args.Term < rf.CurrentTerm || args.CandidateId == rf.me {
		rf.mu.Unlock()
		return
	}
	if args.Term > rf.CurrentTerm {
		//if term T > current term: set current term =t, convert
		rf.stepDown(args.Term)
	}

	if args.LastLogIndex == -1 && args.LastLogTerm == -1 {
		if rf.VotedFor == -1 || rf.VotedFor == args.CandidateId {
			// how to check if the candidate has the most up-to-date log
			// do i need to consider for 2A
			reply.VoteGranted = true
			reply.Term = rf.CurrentTerm
			rf.VotedFor = args.CandidateId

		}
	} else {
		lastLogIndex := rf.Log[len(rf.Log)-1].Index    // the current server, not the one requesting the vote
		lastLogTerm := rf.Log[len(rf.Log)-1].EntryTerm // the current server, not the one requesting the vote

		//the candidate log is not as upto date as the server(the one granting votes)
		if args.LastLogTerm < lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex) {
			// not granting vote and return the current term
			// the current term would be equal, so does not make the candidate turn to follower
			rf.mu.Unlock()
			return
		}
		if rf.VotedFor == -1 || rf.VotedFor == args.CandidateId {
			// how to check if the candidate has the most up-to-date log
			// do i need to consider for 2A
			reply.VoteGranted = true
			reply.Term = rf.CurrentTerm
			rf.VotedFor = args.CandidateId

		}
	}
	rf.mu.Unlock()
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	//log.Println("351 Term(cand)", rf.CurrentTerm, " server(cand) ", rf.me, " State ", rf.State, " sending request vote to ", server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	index := -1
	term := -1
	isLeader := true

	term, isLeader = rf.GetState()
	// Your code here (2B).

	if !isLeader {

		return index, term, false
	}
	rf.mu.Lock()
	index = rf.Log[len(rf.Log)-1].Index + 1
	// get the index of the last log, and increament by 1, because the log index might not be the
	// same as the actual index in the slice/list
	log.Println("Start() server", rf.me, "len before adding command", len(rf.Log))
	rf.Log = append(rf.Log, LogEntry{Index: index, EntryTerm: term, LogCommand: command})
	rf.NextIndex[rf.me] += 1
	rf.MatchIndex[rf.me] = index
	log.Println("Start server", rf.me, "add new command to log len", len(rf.Log))
	rf.mu.Unlock()
	for server := range rf.peers {
		if server != rf.me {
			// do I need a go ?
			go rf.helpApEtReplySend(server, term)
		}
	}

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
// to do
func (rf *Raft) ticker() {

	for !rf.killed() {
		rf.mu.Lock()
		// for leader
		stepDownbool := false
		if rf.State == Leader {
			/**********************************Leader Job-> heartbeat + Append Entries********************************************************/
			rf.ElectionTime = NewElectionTime()
			log.Println("Server", rf.me, "Starting heartbeat/appendEntries")
			rf.mu.Unlock()

			for server := range rf.peers {
				if server != rf.me {
					log.Println("Server(leader)", rf.me, "to server", server, "nextindex", rf.NextIndex)
					// the new helper function; the previous lines will be replaced by the helper when it is completed
					go rf.helpApEtReplySend(server, rf.CurrentTerm) // the reply and args for the actual append entries will be dealt inside the helper

				}
			}
			// rf.mu.Lock()
			// if rf.State != Leader {
			// 	log.Println("Server", rf.me, "term", rf.CurrentTerm, " stepped down/replaced")
			// 	rf.mu.Unlock()
			// 	continue

			// }

			//rf.mu.Unlock()
			time.Sleep(time.Millisecond * 100)

		} else if rf.ElectionTime.After(time.Now()) && rf.State != Leader {
			/*********************************** None Leader -> Sleep until Election Timeout***********************************************/
			timeToEl := time.Until(rf.ElectionTime) //rf.ElectionTime.Sub(time.Now())
			rf.mu.Unlock()
			time.Sleep(timeToEl)
			//time.Sleep(time.Until(rf.ElectionTime))
		} else {

			/********************************************Follower-> Candidate***********************************************/
			log.Println("469 Term ", rf.CurrentTerm, " Server ", rf.me, " becomes transitions from follower to candidate ")
			rf.State = Candidate
			rf.CurrentTerm = rf.CurrentTerm + 1 //Increment currentTerm
			rf.VotedFor = rf.me                 //Vote for self
			rf.ElectionTime = NewElectionTime() //Reset election timer
			rf.mu.Unlock()

			votes := 1 // voting for them self
			/***************************************request votes**************************************************/
			for peer := range rf.peers {
				if peer != rf.me {
					// requestvote arguments for current rf
					args := RequestVoteArgs{}
					if len(rf.Log) < 1 {
						args = RequestVoteArgs{
							Term:         rf.CurrentTerm,
							CandidateId:  rf.me,
							LastLogIndex: -1,
							LastLogTerm:  -1,
						}
					} else {
						args = RequestVoteArgs{
							Term:         rf.CurrentTerm,
							CandidateId:  rf.me,
							LastLogIndex: rf.Log[len(rf.Log)-1].Index,
							LastLogTerm:  rf.Log[len(rf.Log)-1].EntryTerm,
						}
					}

					reply := RequestVoteReply{}

					// deleted go keyword for rf.send request
					log.Println("Term", rf.CurrentTerm, "server", rf.me, "send request to", peer)
					rf.sendRequestVote(peer, &args, &reply)
					log.Println("Term", rf.CurrentTerm, "server(me) ", rf.me, "from", peer, "success:", reply.VoteGranted)

					rf.mu.Lock()
					if reply.Term > rf.CurrentTerm {
						log.Println("Term ", rf.CurrentTerm, " server ", rf.me, ",steping down, reply.term", reply.Term)
						rf.stepDown(reply.Term)
						stepDownbool = true

					}
					if rf.State != Candidate || rf.killed() {
						log.Println("Server", rf.me, "gives up the election")
						rf.mu.Unlock()
						stepDownbool = true
						break
					}
					rf.mu.Unlock()
					if reply.VoteGranted {
						//increament votes if peer granted vote to candidate
						votes++
					}

				}

			}
			rf.mu.Lock()
			if !rf.ElectionTime.After(time.Now()) || stepDownbool {
				// got votes but electiontime has occured
				rf.ElectionTime = NewElectionTime()
				rf.mu.Unlock()
				continue
			}
			rf.mu.Unlock()
			/******************************************establish election*********************************************************/

			if votes > len(rf.peers)/2 {
				rf.mu.Lock()
				log.Println("535 Term ", rf.CurrentTerm, "Server ", rf.me, " got votes ", votes, "/", len(rf.peers), " won election")
				rf.State = Leader // Candidated -> Leader
				lastLogIndex := len(rf.Log) - 1

				// only the leader has this states, the followers and candidates have this as nil
				rf.NextIndex = make([]int, len(rf.peers))
				//(initialized to leader last log index + 1
				for i := range rf.NextIndex {
					rf.NextIndex[i] = lastLogIndex + 1
				}

				rf.MatchIndex = make([]int, len(rf.peers))
				//(initialized to 0, increases monotonically)
				for i := range rf.MatchIndex {
					rf.MatchIndex[i] = 0
				}
				rf.MatchIndex[rf.me] = lastLogIndex
				rf.mu.Unlock()
				// sent the initial appendentries right after becoming the leader to all peers
				for server := range rf.peers {
					if server != rf.me {
						log.Println("Sent initial heartbeat leader", rf.me, "to server", server, "Log len", len(rf.Log))
						go rf.helpApEtReplySend(server, rf.CurrentTerm)

					}
				}

			} else {
				log.Println("Server", rf.me, "failed election retry")

				continue
			}

			//end of the else election time has passed
		} //line453 else when election timeout has occured
	}

	// end of the !killed for loop
}

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
	rf := &Raft{
		peers:        peers,
		persister:    persister,
		me:           me,
		dead:         0,
		State:        Follower,
		CommitSignal: make(chan bool),

		CurrentTerm: 0,
		VotedFor:    -1, // use -1 to represent null/nil
		Log:         []LogEntry{},

		ElectionTime: NewElectionTime(),

		CommitIndex: 0,
		LastApplied: 0,
		// only leader uses this parts
		NextIndex:  nil,
		MatchIndex: nil,
	}

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// LogEntry format {Index, EntryTerm, LogCommand}
	rf.Log = append(rf.Log, LogEntry{0, rf.CurrentTerm, nil})
	applyCh <- ApplyMsg{
		CommandValid: true,
		Command:      rf.Log[0].LogCommand,
		CommandIndex: rf.Log[0].Index,
	}

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.Commiter(rf.CommitSignal, applyCh)

	return rf
}
