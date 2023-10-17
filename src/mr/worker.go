package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type KeyValue struct {
	Key   string
	Value string
}

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	registerWorkerReply := RegisterWorkerReply{}
	ok := call("Coordinator.RegisterWorker", &RegisterWorkerArgs{}, &registerWorkerReply)
	if !ok {
		log.Fatal("Worker unknown: Fail to register worker, exiting...")
		return
	}
	workerId := registerWorkerReply.Id
	nReduce := registerWorkerReply.ReduceCnt
	log.Printf("Worker %v: received Id and ReduceCnt: %v\n", workerId, nReduce)
	for {
		askForTaskReply := AskForTaskReply{}
		ok := call("Coordinator.AskForTask", &AskForTaskArgs{}, &askForTaskReply)
		if !ok {
			log.Fatal("Worker %v: unable to connect to coordinator\n", workerId)
			return
		}
		taskType := askForTaskReply.TaskType
		taskPath := askForTaskReply.FilePath
		taskId := askForTaskReply.TaskId
		log.Printf("Worker %v: receive task assignment from coordinator, task type: %v,"+
			"task path: %v, task id: %v.\n", workerId, taskType, taskPath, taskId)
		if taskType == "map" {
			reduceToFileMap := createTempFileForMapOutput(nReduce, taskId)
			log.Printf("Worker %v: created temporary intermediate file.\n", workerId)
			keyValuePairs := runMapTask(taskPath, mapf)
			sort.Sort(ByKey(keyValuePairs))
			log.Printf("Worker %v: ran map tasks and sorted intermediate result.\n", workerId)
			writeTempFiles(keyValuePairs, nReduce, reduceToFileMap)
			log.Printf("Worker %v: created kv pairs and wrote to temporary files.\n", workerId)
			closeAllTempFiles(reduceToFileMap)
			renameTempFile(reduceToFileMap, taskId)
			log.Printf("Worker %v: finished renaming temporary files.\n", workerId)
		} else if taskType == "reduce" {
			taskFilePathList := askForTaskReply.FilePathList
			log.Printf("Worker %v: received file path list for %v task %v, file list: %v\n",
				workerId, taskType, taskId, taskFilePathList)
			keyValueList := extractKeyValuePairs(taskFilePathList)
			//log.Printf("Worker %v: extracted key value pairs: %v",
			//	workerId, keyValueList)
			runReduceTaskAndSaveResult(keyValueList, taskId, reducef)
			log.Printf("Worker %v: ran reduce task %v and output result to files.\n", workerId, taskId)
		} else {
			log.Printf("worker %v: received null reply for AskForType", workerId)
			getPhaseReply := GetPhaseReply{}
			ok = call("Coordinator.GetPhase", &GetPhaseArgs{}, &getPhaseReply)
			if !ok {
				log.Fatal("Worker %d: Fail to get coordinator's status, exit", workerId)
				return
			}
			log.Printf("worker %v: received phase from coordinator: %v", workerId, getPhaseReply.Phase)
			if getPhaseReply.Phase == "done" {
				log.Printf("worker %v: received done from coordinator, exit", workerId, getPhaseReply.Phase)
				return
			}
			continue
		}
		ok = call("Coordinator.MarkAsDone", &MarkAsDoneArgs{FilePath: taskPath, TaskType: taskType}, &MarkAsDoneReply{})
		if !ok {
			log.Fatalf("worker %v: failed to mark task %v as done", workerId, taskId)
		}
		log.Printf("Worker %v: mark %v task %v as done.\n", workerId, taskType, taskId)
		time.Sleep(1000)
	}
}

func writeTempFiles(keyValuePairs []KeyValue, nReduce int, reduceToFileMap map[int]*os.File) {
	for _, kv := range keyValuePairs {
		k := kv.Key
		kHash := ihash(k) % nReduce
		targetFile := reduceToFileMap[kHash]
		enc := json.NewEncoder(targetFile)
		err := enc.Encode(&kv)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func closeAllTempFiles(reduceToFileMap map[int]*os.File) {
	for _, v := range reduceToFileMap {
		v.Close()
	}
}

func extractKeyValuePairs(taskFilePathList []string) []KeyValue {
	keyValueList := []KeyValue{}
	log.Printf("%v", taskFilePathList)
	for _, fileName := range taskFilePathList {
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatal(err)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				file.Close()
				break
			}
			keyValueList = append(keyValueList, kv)
		}
	}
	return keyValueList
}

func renameTempFile(fileMap map[int]*os.File, taskId int) {
	for k, v := range fileMap {
		oldName := v.Name()
		newName := fmt.Sprintf("mr-%v-%v", taskId, k)
		err := os.Rename(oldName, newName)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func createTempFileForMapOutput(nReduce int, taskId int) map[int]*os.File {
	m := make(map[int]*os.File)
	for i := 0; i < nReduce; i++ {
		tempFileName := fmt.Sprintf("temp-%v-%v-*", taskId, i)
		f, err := os.CreateTemp("", tempFileName)
		if err != nil {
			log.Fatal(err)
		}
		m[i] = f
	}
	return m
}

func runMapTask(taskPath string, f func(string, string) []KeyValue) []KeyValue {
	file, err := os.Open(taskPath)
	if err != nil {
		log.Fatalf("cannot open %v", taskPath)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", taskPath)
	}
	file.Close()
	return f(taskPath, string(content))
}

func runReduceTaskAndSaveResult(keyValuePairs []KeyValue, taskId int, f func(string, []string) string) {
	sort.Sort(ByKey(keyValuePairs))
	i := 0
	saveFileTempNamePattern := fmt.Sprintf("mr-out-%v*", taskId)
	saveFile, err := os.CreateTemp("", saveFileTempNamePattern)
	tempFileName := saveFile.Name()
	for i < len(keyValuePairs) {
		j := i + 1
		for j < len(keyValuePairs) && keyValuePairs[j].Key == keyValuePairs[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, keyValuePairs[k].Value)
		}
		output := f(keyValuePairs[i].Key, values)

		if err != nil {
			log.Fatal(err)
		}
		fmt.Fprintf(saveFile, "%v %v\n", keyValuePairs[i].Key, output)
		i = j
	}
	saveFileName := fmt.Sprintf("mr-out-%v", taskId)
	err = os.Rename(tempFileName, saveFileName)
	if err != nil {
		log.Fatal(err)
	}
	saveFile.Close()
}

func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
