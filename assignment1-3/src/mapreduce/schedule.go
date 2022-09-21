package mapreduce

import (
	"fmt"
	"sync"
)

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	debug("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//

	//fmt.Println("\nSchedule " + phase)

	// Add jobs to pendingTasks
	pendingTasksChan := make(chan DoTaskArgs, ntasks+1)
	for i := 0; i < ntasks; i++ {
		pendingTasksChan <- DoTaskArgs{
			JobName:       mr.jobName,
			File:          mr.files[i],
			Phase:         phase,
			TaskNumber:    i,
			NumOtherPhase: nios,
		}
	}

	var waitGroup sync.WaitGroup
	waitGroup.Add(ntasks)

	// listening to workers channel for availability
	for i := 0; i < ntasks; i++ {
		go func() {
			phase := phase
			executeWorker(mr.registerChannel, pendingTasksChan, &waitGroup, string(phase))
		}()
	}

	// Waiting for all the jobs to execute
	waitGroup.Wait()
	//fmt.Println("startScheduler finished " + phase)

	debug("Schedule: %v phase done\n", phase)
}

func executeWorker(
	registerChannel chan string,
	pendingTasksChan chan DoTaskArgs,
	waitGroup *sync.WaitGroup,
	phase string) {

	// read available worker and read pending Tasks

	reply := ShutdownReply{}
	workerName := <-registerChannel
	//fmt.Println("[inside for] worker pulled" + workerName + " : " + phase)

	taskArgs := <-pendingTasksChan
	//fmt.Println("[inside for] task pulled" + string(rune(taskArgs.TaskNumber)) + " : " + phase)

	ok := call(workerName, "Worker.DoTask", taskArgs, &reply)
	if ok == false {
		// task is not done. add it back to pending
		fmt.Println("[RPC fail] adding back to pending" + phase)
		go func() {
			pendingTasksChan <- taskArgs
			executeWorker(registerChannel, pendingTasksChan, waitGroup, phase)
		}()

	} else {
		// task is complete, release the waitGroup
		waitGroup.Done()
		//fmt.Println("[inside for] waitgroup Done " + phase)

		// add the free worker to available channel queue
		registerChannel <- workerName
		//fmt.Println("[inside for] worker added back " + workerName + " " + phase)
	}
}
