package shared

import (
	//    "fmt"
	"CSC569lab4/shared"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"plugin"
	"sync"
	"time"
)

const (
    MAX_NODES      =  8 //update as needed
    Z_TIME_MIN     = 10
    ROLE_FOLLOWER  =  0
    ROLE_CANDIDATE =  1
    ROLE_LEADER    =  2

    TASK_MAP    = "map"
    TASK_REDUCE = "reduce"
    TASK_WAIT   = "wait"

    TASK_IDLE       = "idle"
    TASK_INPROGRESS = "inprogress"
    TASK_COMPLETE   = "complete"
    PHASE_COMPLETE   = "complete"
)

type KeyValue struct {
	Key   string
	Value string
}

// Node struct represents a computing node.
type Node struct {
    ID        int
    Hbcounter int
    Time      float64
    Alive     bool
    Term      int
    Role      int //Could be enum type or constants
    LeaderID  int
    Voted     bool
}

// Generate random crash time from 10-60 seconds
func (n Node) CrashTime() int {
    rand.Seed(time.Now().Unix())
    max := 60
    min := 10
    return rand.Intn(max-min) + min
}

func (n Node) InitializeNeighbors(id int) [2]int {
    neighbor1 := RandInt()
    for neighbor1 == id {
        neighbor1 = RandInt()
    }
    neighbor2 := RandInt()
    for neighbor1 == neighbor2 || neighbor2 == id {
        neighbor2 = RandInt()
    }
    return [2]int{neighbor1, neighbor2}
}

func (leader *Node) SetLeader(payload Node, reply *Node) error {
    if leader.Term < payload.Term && payload.Alive {
        *leader = payload
        *reply = *leader
        return nil
    }
    return errors.New("Suggested leader does not have new term")
}

func (leader *Node) GetLeader(payload Node, reply *Node) error {
    if leader.Term >= payload.Term { //should be equal as result of election
        payload.LeaderID = leader.ID
        *reply = payload
        return nil
    }
    *reply = payload
    return errors.New("No new leader")
}

func RandInt() int {
    rand.Seed(time.Now().Unix())
    return rand.Intn(MAX_NODES-1+1) + 1
}

/*---------------*/

// Election struct represents a candidate Election for RAFT election
type Election struct {
    Proposals map[int]Node // What node each node is voting for as leader
    Votes     map[int]int // Number of votes each node has received
    Term      int
    mu        sync.Mutex
}

// Returns a new instance of a Election (pointer).
func NewElection() *Election {
    return &Election{
        Proposals: make(map[int]Node),
        Votes: make(map[int]int),
        Term: 1,
    }
}

// Adds a proposal to the proposals list.
func (p *Election) Enqueue(payload Node, reply *Node) error { //Proposals are the mailboxes
    // Go through all mailboxes and see if they have a proposal with a term less than prposal
    // If term is higher, don't update mailbox w proposal
    // If term is less or equal, add proposal to mailbox
    p.mu.Lock()
    for id := 1; id <= MAX_NODES; id++ {
        proposal, exists := p.Proposals[id]
        if !exists || (payload.Term > proposal.Term) { //Add proposal if empty or has greater term
            p.Proposals[id] = payload //First of that term, add
        }
    }
    *reply = payload
    p.mu.Unlock()
    return nil
}

// get the node proposal at the payload node's id and check if the term is equal to current term.
func (p *Election) Dequeue(payload Node, reply *Node) error { //Proposals are the mailboxes
    //TODO
    p.mu.Lock()
    if (p.Proposals != nil) {
        currentElection := p.Proposals[payload.ID]
        if(currentElection.Term == payload.Term) {
            *reply = currentElection
            p.mu.Unlock()
            return nil
        } else {
            *reply = Node{} //empty node if not found
            p.mu.Unlock()
            return nil
        }
    }
    *reply = Node{} //If empty, return empty
    p.mu.Unlock()
    return errors.New("Proposals do not exist")
}

func (p *Election) Vote(ID int, reply *bool) error {
    p.mu.Lock()
    p.Votes[ID]++
    p.mu.Unlock()
    *reply = true
    return nil
}

func (p *Election) Clear(term int, response *bool) error {
    if (term > p.Term) { //Update term
        for vote := range p.Votes {
            delete(p.Votes, vote)
        }
        p.Term++
        *response = true
        return nil
    }
    *response = false
    return nil

}

func (p *Election) CountVotes(ID int, reply *int) error {
    *reply = p.Votes[ID]
    return nil
}

/*--------------*/


// Membership struct represents participanting nodes
type Membership struct {
    Members map[int]Node
    mu      sync.Mutex
}

// Returns a new instance of a Membership (pointer).
func NewMembership() *Membership {
    return &Membership{
        Members: make(map[int]Node),
    }
}

// Adds a node to the membership list.
func (m *Membership) Add(payload Node, reply *Node) error {
    //TODO
    m.mu.Lock()
    if (m.Members != nil) {
        m.Members[payload.ID] = payload
        *reply = payload //May need to change HB counter/node vars
        m.mu.Unlock()
        return nil
    }
    m.mu.Unlock()
    return errors.New("Members does not exist")
}

// Updates a node in the membership list.
func (m *Membership) Update(payload Node, reply *Node) error {
    //TODO
    m.mu.Lock()
    m.Members[payload.ID] = payload
    m.mu.Unlock()
    *reply = payload
    return nil //errors.New("\"Update\" unimplemented")
}

func (m *Membership) Get(payload int, reply *Node) error {
    //TODO
    m.mu.Lock()
    val, exists := m.Members[payload] //Map fetches return two values!!

    if !exists { // Return error if node does not exist
        m.mu.Unlock()
        return errors.New("Node " + fmt.Sprintf("%d", payload) + " does not exist in members")
    }

    *reply = val

    m.mu.Unlock()
    return nil
}

/*---------------*/

// Request struct represents a new message request to a client
type Request struct {
    ID    int
    Table Membership
}

// Requests struct represents pending message requests
type Requests struct {
    Pending map[int]Request
    mu    sync.Mutex
}

// Returns a new instance of a Membership (pointer).
func NewRequests() *Requests {
    //TODO
    return &Requests{
        Pending: make(map[int]Request),
    }
}

// Adds a new message request to the pending list
func (req *Requests) Add(payload Request, reply *bool) error {
    //TODO
    req.mu.Lock()
    req.Pending[payload.ID] = payload
    *reply = true // Does this have a point?
    req.mu.Unlock()
    return nil
}

// Listens to communication from neighboring node & returns table
func (req *Requests) Listen(ID int, reply *Membership) error {
    //TODO
    //Interpret ID as node listened to
    req.mu.Lock()
    request, exists := req.Pending[ID]
    if exists {
        *reply = request.Table
        req.mu.Unlock()
        return nil
    }
    req.mu.Unlock()

    return errors.New("Error: Requests.Listen() pending message from node '" + fmt.Sprintf("%d", ID) + "' does not exist")
}

func CombineTables(primary *Membership, other *Membership) *Membership {
    //TODO

    original := NewMembership()

    //Duplicate primary
    for ID, node := range primary.Members {
        original.Members[ID] = node
    }
    currTime := float64(time.Now().Unix())

    for ID, nodeO := range other.Members {
        nodeP, exists := primary.Members[ID]
        if exists { //if node exists in primary
            if nodeP.Hbcounter < nodeO.Hbcounter {
                nodeP.Hbcounter = nodeO.Hbcounter
                nodeP.Alive = true
                nodeP.Time = currTime
                primary.Members[ID] = nodeP
            }
        } else { //Need to add new node
            //Need to change before assignment since go maps can't be modified
            nodeO.Time = float64(time.Now().Unix()) // Time converted from time.Time to float
            primary.Members[ID] = nodeO
        }
    }

    // Check if heartbeat has increased from original
    for ID, nodeN := range primary.Members {
        nodeO, exists := original.Members[ID]
        if exists {     //If node was just inserted, assume alive
            if (nodeN.Hbcounter == nodeO.Hbcounter) &&
                ((currTime - nodeO.Time) >= Z_TIME_MIN) {
                nodeN.Alive = false
                primary.Members[ID] = nodeN
            }
        }
    }
    return primary
}

/*---------------*/
// MapReduce and Log Replication implementation

func LoadPlugin(filename string) (func(string, string) []shared.KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []shared.KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}


type LogEntry struct {
    Index int
    Term  int
    Task  Task
}

type Task struct {
    ID int
    ShardNo int
    ShardStart int
    ShardEnd int
    TypeOfTask string
    Filename string
    Term int
    Status string
    WorkerID int
    LeaderID int
}

type TaskAssignments struct {
    WorkerTasks map[int]Task // workerID -> current task for that worker
    AllTasks    map[int]Task // taskID -> task
    Phase       string        // "map", "reduce", or "complete"
    mu          sync.Mutex
}

func NewTaskAssignments() *TaskAssignments {
    return &TaskAssignments{
        WorkerTasks: make(map[int]Task),
        AllTasks:    make(map[int]Task),
        Phase:       TASK_MAP,
    }
}

func (m *TaskAssignments) AddTasks(tasks []Task, reply *bool) error {
    m.mu.Lock()
    defer m.mu.Unlock()

    for _, task := range tasks {
        if _, exists := m.AllTasks[task.ID]; !exists {
            m.AllTasks[task.ID] = task
        }
    }

    *reply = true
    return nil
}

func (m *TaskAssignments) AssignTask(task Task, reply *bool) error {
    m.mu.Lock()
    defer m.mu.Unlock()

    task.Status = TASK_INPROGRESS
    m.WorkerTasks[task.WorkerID] = task
    m.AllTasks[task.ID] = task

    *reply = true
    return nil
}

func (m *TaskAssignments) GetTask(workerID int, reply *Task) error {
    m.mu.Lock()
    defer m.mu.Unlock()

    task, exists := m.WorkerTasks[workerID]
    if !exists || task.Status == TASK_COMPLETE || task.Status == TASK_IDLE {
        *reply = Task{TypeOfTask: TASK_WAIT}
        return nil
    }

    *reply = task
    return nil
}

func (m *TaskAssignments) CompleteTask(workerID int, reply *bool) error {
    m.mu.Lock()
    defer m.mu.Unlock()

    task, exists := m.WorkerTasks[workerID]
    if !exists {
        *reply = false
        return nil
    }

    task.Status = TASK_COMPLETE
    m.WorkerTasks[workerID] = task
    m.AllTasks[task.ID] = task

    *reply = true
    return nil
}

func (m *TaskAssignments) FailTask(workerID int, reply *bool) error {
    m.mu.Lock()
    defer m.mu.Unlock()

    task, exists := m.WorkerTasks[workerID]
    if !exists {
        *reply = false
        return nil
    }

    delete(m.WorkerTasks, workerID)

    task.Status = TASK_IDLE
    task.WorkerID = 0
    m.AllTasks[task.ID] = task

    *reply = true
    return nil
}

func (m *TaskAssignments) GetWorkerTasks(dummy int, reply *map[int]Task) error {
    m.mu.Lock()
    defer m.mu.Unlock()

    copyTasks := make(map[int]Task)
    for workerID, task := range m.WorkerTasks {
        copyTasks[workerID] = task
    }

    *reply = copyTasks
    return nil
}

func (m *TaskAssignments) GetIdleTask(taskType string, reply *Task) error {
    m.mu.Lock()
    defer m.mu.Unlock()

    for _, task := range m.AllTasks {
        if task.TypeOfTask == taskType && task.Status == TASK_IDLE {
            *reply = task
            return nil
        }
    }

    *reply = Task{TypeOfTask: TASK_WAIT}
    return nil
}

func (m *TaskAssignments) AllComplete(taskType string, reply *bool) error {
    m.mu.Lock()
    defer m.mu.Unlock()

    found := false

    for _, task := range m.AllTasks {
        if task.TypeOfTask != taskType {
            continue
        }

        found = true

        if task.Status != TASK_COMPLETE {
            *reply = false
            return nil
        }
    }

    *reply = found
    return nil
}

func (m *TaskAssignments) GetPhase(dummy int, reply *string) error {
    m.mu.Lock()
    defer m.mu.Unlock()

    *reply = m.Phase
    return nil
}

func (m *TaskAssignments) SetPhase(phase string, reply *bool) error {
    m.mu.Lock()
    defer m.mu.Unlock()

    m.Phase = phase
    *reply = true
    return nil
}

func (m *TaskAssignments) IsEmpty(dummy int, reply *bool) error {
    m.mu.Lock()
    defer m.mu.Unlock()

    *reply = len(m.AllTasks) == 0
    return nil
}