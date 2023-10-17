package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	nReduce int
	phase   string
	mutex   sync.Mutex

	workerCnt  int
	rawTasks   map[string]string
	reduceTask map[string]string
	taskIds    map[string]int
}

func (c *Coordinator) RegisterWorker(args *RegisterWorkerArgs, reply *RegisterWorkerReply) error {
	c.mutex.Lock()
	id := c.workerCnt
	c.workerCnt++
	c.mutex.Unlock()
	reply.Id = id
	reply.ReduceCnt = c.nReduce
	log.Printf("Coordinator: received new worker, assign id %v with ReduceCnt %v\n", id, reply.ReduceCnt)
	return nil
}

func (c *Coordinator) AskForTask(args *AskForTaskArgs, reply *AskForTaskReply) error {
	defer c.mutex.Unlock()
	c.mutex.Lock()
	if c.phase == "map" || c.phase == "reduce" {
		var taskMap *map[string]string
		if c.phase == "map" {
			taskMap = &c.rawTasks
		} else if c.phase == "reduce" {
			taskMap = &c.reduceTask
		}
		log.Printf("Coordinator: receive request for task , phase: %v, taskMap: %v", c.phase, taskMap)
		idleTask := ""
		for k, v := range *taskMap {
			if v == "unassigned" {
				idleTask = k
				break
			}
		}
		if idleTask == "" {
			return nil
		}
		log.Printf("Coordinator: assign %v task %v\n", c.phase, c.taskIds[idleTask])
		reply.FilePath = idleTask
		if c.phase == "reduce" {
			reply.FilePathList = []string{}
			for i := 0; i < len(c.rawTasks); i++ {
				reply.FilePathList = append(reply.FilePathList, fmt.Sprintf("mr-%v-%v", i, c.taskIds[idleTask]))
			}
		}

		reply.TaskType = c.phase
		reply.TaskId = c.taskIds[idleTask]
		(*taskMap)[idleTask] = "assigned"
		log.Printf("Coordinator: assign %v task %v to worker\n", c.phase, c.taskIds[idleTask])
		go c.monitorTask(idleTask, c.phase)
	}

	return nil
}

func (c *Coordinator) GetPhase(args *GetPhaseArgs, reply *GetPhaseReply) error {
	defer c.mutex.Unlock()
	c.mutex.Lock()
	reply.Phase = c.phase
	return nil
}

func (c *Coordinator) MarkAsDone(args *MarkAsDoneArgs, reply *MarkAsDoneReply) error {
	defer c.mutex.Unlock()
	c.mutex.Lock()
	task := args.FilePath
	taskType := args.TaskType
	if taskType != c.phase {
		return nil
	}
	var taskMap *map[string]string
	if taskType == "map" {
		taskMap = &c.rawTasks
	} else if taskType == "reduce" {
		taskMap = &c.reduceTask
	} else {
		return fmt.Errorf("task of unknown type marked as done\n")
	}
	(*taskMap)[task] = "done"
	log.Printf("Coordinator: %v task %v is now marked as done\n", taskType, c.taskIds[task])
	done := true
	for _, v := range *taskMap {
		if v != "done" {
			done = false
			break
		}
	}
	if done {
		if c.phase == "map" {
			log.Printf(
				"Coordinator: all map tasks have finished, preparing reduce tasks\n", taskType, c.taskIds[task])
			c.phase = "reduce"
			cnt := len(c.taskIds)
			for i := 0; i < c.nReduce; i++ {
				c.reduceTask[fmt.Sprintf("%v", i)] = "unassigned"
				c.taskIds[fmt.Sprintf("%v", i)] = i
				cnt++
			}
		} else {
			log.Printf("Coordinator: all reduce tasks have finished, exit the program."+
				"map task map: %v\n reduce task map: %v\n", c.rawTasks, c.reduceTask)
			c.phase = "done"
		}
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) monitorTask(task string, taskType string) {
	done := false
	var taskMap *map[string]string
	if taskType == "map" {
		taskMap = &c.rawTasks
	} else if taskType == "reduce" {
		taskMap = &c.reduceTask
	} else {
		log.Fatal("task of unknown type marked as done")
		return
	}
	for !done {
		time.Sleep(10 * time.Second)
		c.mutex.Lock()
		done = (*taskMap)[task] == "done"
		if !done {
			(*taskMap)[task] = "unassigned"
		}
		c.mutex.Unlock()
	}
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	defer c.mutex.Unlock()
	c.mutex.Lock()
	return c.phase == "done"
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// ReduceCnt is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.nReduce = nReduce
	c.server()
	log.Printf("Coordinator: Launched, receive ReduceCnt: %v\n", nReduce)
	c.mutex.Lock()
	c.phase = "map"
	c.workerCnt = 0
	c.taskIds = make(map[string]int)
	c.rawTasks = make(map[string]string)
	cnt := 0
	for _, f := range files {
		c.rawTasks[f] = "unassigned"
		c.taskIds[f] = cnt
		cnt++
	}
	c.reduceTask = make(map[string]string)
	c.mutex.Unlock()
	//go c.startMap()
	return &c
}
