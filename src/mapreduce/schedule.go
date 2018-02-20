package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//

	// (1) Have a goroutine that loops on registerChan
	// 		and spawn a new pump thread for each worker "server"

	taskChan := make(chan DoTaskArgs)
	wg := sync.WaitGroup{}
	wg.Add(ntasks)

	// I then moved
	// wg.Wait() to here.
	// ... and deadlock obviously

	go func() {
		// This thread loops and manufactures a SchedulableTask
		// "ntask" and appends it to the schedulableTask channel

		// When this thread is done manufacturing tasks,
		// it waits on a waitgroup with count "ntask"

		// Finally, when it wakes up from the waitgroup, it closes
		// registerChan and taskChan
		for i := 0; i < ntasks; i++ {
			sTask := DoTaskArgs{JobName: jobName, File: mapFiles[i], Phase: phase, TaskNumber: i, NumOtherPhase: n_other}
			taskChan <- sTask
		}

		// Originally I had wg logic in this thread,
		// but because this isn't the main thread,
		// main thread exits before this Wait is honored.
		// wg.Add(ntasks)
		// wg.Wait()
	}()

	go func() {
		// This thread loops on registerChan and spawns a new
		// taskPumpThread for every registered worker
		// until the registerChan is explicitly closed.
		for {
			workerName := <-registerChan
			go func() {
				// This thread loops on schedulableTask channel
				// and keeps feeding tasks from this channel
				// to its worker until the channel is explicitly closed.

				// This thread does an rpc "call" to the worker.

				// If the "rpc" call returns
				// 	True: this thread increments the waitgroup
				//	False: this means worker is dead, puts task back on taskChan and exit
				for {
					taskArg, ok := <-taskChan
					if !ok {
						break
					}

					if call(workerName, "Worker.DoTask", taskArg, new(struct{})) {
						wg.Done()
					} else {
						taskChan <- taskArg
					}
				}
			}()
		}
	}()

	wg.Wait()
	close(taskChan)
	// I had to remove the close(registerChan) because Panic send on closed channel
	// due to test driver keeps spawning workers
	// close(registerChan)
	fmt.Printf("Schedule: %v done\n", phase)
}
