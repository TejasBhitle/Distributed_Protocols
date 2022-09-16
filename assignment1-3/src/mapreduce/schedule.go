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

	// Add jobs to pendingTasks
	pendingTasksChan := make(chan DoTaskArgs)
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
	go func() {
		for {
			workerName := <-mr.registerChannel
			taskArgs := <-pendingTasksChan

			ok := call(workerName, "Worker.DoTask", taskArgs, nil)
			if ok == false {
				// task is not done. add it back to pending
				pendingTasksChan <- taskArgs

			} else {
				// task is complete
				waitGroup.Done()
			}
		}
	}()

	// Waiting for all the jobs to execute
	waitGroup.Wait()
	fmt.Println("startScheduler finished " + phase)

	debug("Schedule: %v phase done\n", phase)
}
