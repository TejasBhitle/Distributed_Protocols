package mapreduce

import (
	"fmt"
	"strconv"
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
	pendingTasksChan := make(chan DoTaskArgs, ntasks+1)
	fmt.Println("pendingTasksChan created")
	for i := 0; i < ntasks; i++ {
		pendingTasksChan <- DoTaskArgs{
			JobName:       mr.jobName,
			File:          mr.files[i],
			Phase:         phase,
			TaskNumber:    i,
			NumOtherPhase: nios,
		}
	}

	fmt.Println("tasks created " + strconv.Itoa(ntasks))

	var waitGroup sync.WaitGroup
	waitGroup.Add(ntasks)
	fmt.Println("waitgroup init")

	availableWorkersChan := make(chan string, 1000)
	go func() {
		// if any new worker registers, push it to available workers chan
		fmt.Println("[init availableWorkersChan]")
		for {
			workerName := <-mr.registerChannel
			fmt.Println("[" + workerName + " added to availableWorkersChan]")
			availableWorkersChan <- workerName
		}
	}()

	count := 0 // todo delete later
	// listening to workers channel for availability
	go func() {
		for {
			//fmt.Println("[inside for]")
			workerName := <-availableWorkersChan
			taskArgs := <-pendingTasksChan
			//fmt.Println("[inside for] " + workerName + " : " + taskArgs.JobName)

			ok := call(workerName, "Worker.DoTask", taskArgs, nil)
			if ok == false {
				// task is not done. add it back to pending
				pendingTasksChan <- taskArgs
				fmt.Println("[inside for] adding back to pending")

			} else {
				// task is complete
				//fmt.Println("[inside for] waitgroup Done")
				waitGroup.Done()
				count++

				// add the free worker to available channel queue
				availableWorkersChan <- workerName
			}
		}
	}()

	// Waiting for all the jobs to execute
	waitGroup.Wait()
	fmt.Println("startScheduler finished " + phase + " : ")

	debug("Schedule: %v phase done\n", phase)
}
