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
import "labrpc"
import "math/rand"
import "time"
//import "fmt"
// import "bytes"
// import "labgob"



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
	
	//add property
	Term         int
	Success      bool
}


type AppendEntries struct {
	Term         int
	LeaderId     int
	PrevLogIndex int 
	PrevLogTerm  int
	//entries
	IsHeartBeat  bool
    LeaderCommit int
}


//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm   int
    voteForId     int
    lastLogIndex  int
    lastVoteTime  int64
    status        int //0:follower 1:candidate 2:leader
    applyCh       chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	term = rf.currentTerm

	//fmt.Printf("me:%d,term:%d\n",rf.me,term)

	isleader = (rf.status == 2)
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




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
    CandidateId  int
    LastLogIndex int
    LastlogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term         int
	VoteGranted  bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	//fmt.Printf("me:%d,status:%d,term:%d,CandidateId:%d.\n",rf.me,rf.status,args.Term,args.CandidateId)
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
    if(args.Term < rf.currentTerm || rf.status == 2){
    	reply.VoteGranted = false
    	rf.lastVoteTime = time.Now().Unix()
    	//fmt.Printf("me:%d,not vote for %d.\n",rf.me,args.CandidateId)
    	rf.mu.Unlock()
    	return
    }
    //if((rf.voteForId == 0 || rf.voteForId == args.CandidateId) && (args.LastLogIndex >= rf.lastLogIndex)){
    	reply.VoteGranted = true
    	rf.voteForId = args.CandidateId
    	rf.status = 0
    	rf.lastVoteTime = time.Now().Unix()
    	rf.currentTerm = args.Term
    	//fmt.Printf("me:%d,vote for %d.\n",rf.me,args.CandidateId)
    //}else{
    	//fmt.Printf("me:%d,not vote for %d.\n",rf.me,args.CandidateId)
    //}
    rf.mu.Unlock()
}


func (rf *Raft) RequestAppendEntries(args *AppendEntries, reply *ApplyMsg) {
	rf.mu.Lock()
	//fmt.Printf("hear beate,me:%d\n",rf.me)
	if(args.IsHeartBeat){
		rf.lastVoteTime = time.Now().Unix()
		rf.currentTerm = args.Term
		rf.status = 0
	}
	reply.Success = true
	rf.mu.Unlock()
}

func (rf *Raft) RequestAppendLog(args *ApplyMsg, reply *ApplyMsg) {
	rf.mu.Lock()
	//fmt.Printf("hear beate,me:%d\n",rf.me)
	go func() {
		rf.applyCh <- *args
	}()

	reply.Success = true
	rf.mu.Unlock()
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


func (rf *Raft) sendRequestAppendEntries(server int, args *AppendEntries, reply *ApplyMsg) bool {
	ok := rf.peers[server].Call("Raft.RequestAppendEntries", args, reply)
	return ok
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

	// Your code here (2B).
	rf.mu.Lock()
	if(rf.status != 2){
		isLeader = false;
	}else{
		term = rf.currentTerm
		rf.lastLogIndex++
		index = rf.lastLogIndex
		count := len(rf.peers)
		for i := 0; i < count; i++ {
			tmpindex := i
			go func() {
				request := &ApplyMsg{}
				request.CommandValid = true
				request.Command = command
				request.CommandIndex = index
				response := &ApplyMsg{}
				rf.peers[tmpindex].Call("Raft.RequestAppendLog", request, response)
			}()
		}
	}

	rf.mu.Unlock()

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
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
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
    // goroutine 
    // lasttime
    // 超时，发送 投票请求,根据投票结果
	// initialize from state persisted before a crash
	rf.lastVoteTime = 0
	rf.currentTerm = 0
	rf.status = 0 //follower
	rf.applyCh = applyCh
	go func() {
	//loop:
		for {
			nowtime := time.Now().Unix()
			if(nowtime - rf.lastVoteTime > 2){
				//sleep
				rand.Seed(time.Now().UnixNano())
				randtime := rand.Intn(150) + 150;
				//fmt.Printf("rnadindex:%d\n",randtime)
				time.Sleep(time.Duration(randtime) * time.Millisecond)

				rf.mu.Lock()
				nowtime = time.Now().Unix()
				if(rf.status == 0 && nowtime - rf.lastVoteTime > 2){
					//vote
					args := &RequestVoteArgs{}
					tmpTermId := rf.currentTerm + 1
					args.Term = tmpTermId 
					args.CandidateId = me
					count := len(peers)
					agreecount := 0
					rf.status = 1 //candidate
					nowtime = time.Now().Unix()
					rf.lastVoteTime = nowtime
					//fmt.Printf("candidate:me:%d,status:%d,term:%d\n",rf.me,rf.status,tmpTermId)
					for i := 0; i < count; i++ {
						if(i != me){
							tmpindex := i
							go func(){
								reply := &RequestVoteReply{}
								//fmt.Printf("index:%d\n",i)
								peers[tmpindex].Call("Raft.RequestVote", args, reply)
								if(reply.VoteGranted){
									agreecount++
									if(agreecount >= count/2 && rf.status == 1){
										rf.status = 2 //leader
										rf.currentTerm = tmpTermId 
										//heart beart to all follers
										heartBeatArgs := &AppendEntries{}
										heartBeatArgs.IsHeartBeat = true
										heartBeatArgs.Term = rf.currentTerm
										for j := 0; j < count; j++ {
											if(j != me){
												heartBeattmpindex := j
												go func(){
													heartBeatreply := &ApplyMsg{}
													peers[heartBeattmpindex].Call("Raft.RequestAppendEntries", heartBeatArgs, heartBeatreply)
												}()
											}
										}
										rf.lastVoteTime = time.Now().Unix()
									}
								}
							}()
						}
					}
				}
				rf.mu.Unlock()
			}

			//don't need to wait for all the vote reply
			rf.mu.Lock()
			if(rf.status == 2){//leader
				nowtime = time.Now().Unix()
				if(nowtime - rf.lastVoteTime > 1){
					count := len(peers)
					//fmt.Printf("send heart beat index:%d,count:%d,status:%d.\n",rf.me,count,rf.status)
					args := &AppendEntries{}
					args.IsHeartBeat = true
					args.Term = rf.currentTerm
					rf.lastVoteTime = time.Now().Unix()
					for i := 0; i < count; i++ {
						if(i != me){
							tmpindex := i
							go func(){
								reply := &ApplyMsg{}
								peers[tmpindex].Call("Raft.RequestAppendEntries", args, reply)
							}()
						}
					}
					rf.lastVoteTime = time.Now().Unix()
				}
			}

			rf.mu.Unlock()

			time.Sleep(10 * time.Millisecond)
		}
	}()

	rf.readPersist(persister.ReadRaftState())


	return rf
}
