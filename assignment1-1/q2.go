package cos418_hw1_1

import (
	"bufio"
	"io"
	"log"
	"os"
	"strconv"
	"sync"
)

const ConstChannelBuffer = 1005

// Sum numbers from channel `nums` and output sum to `out`.
// You should only output to `out` once.
// Do NOT modify function signature.
func sumWorker(nums chan int, out chan int) {
	// TODO: implement me
	// HINT: use for loop over `nums`

	//fmt.Println("SumWorker started")
	sum := 0
	for num := range nums {
		sum += num
	}
	//fmt.Printf("Sum is %v\n", sum)
	out <- sum
}

// Read integers from the file `fileName` and return sum of all values.
// This function must launch `num` go routines running
// `sumWorker` to find the sum of the values concurrently.
// You should use `checkError` to handle potential errors.
// Do NOT modify function signature.
func sum(numOfChannels int, fileName string) int {
	// TODO: implement me
	// HINT: use `readInts` and `sumWorkers`
	// HINT: used buffered channels for splitting numbers between workers

	inputFile, fileError := os.Open(fileName)
	if fileError != nil {
		log.Fatal(fileError)
	}

	nums, readError := readInts(inputFile)
	if readError != nil {
		log.Fatal(readError)
	}

	// Init the channels
	var channels []chan int
	for i := 0; i < numOfChannels; i++ {
		channels = append(channels, make(chan int, ConstChannelBuffer))
	}

	// push elements in a cyclic fashion into the channels
	currentChannelIndex := 0
	for _, x := range nums {
		channels[currentChannelIndex%numOfChannels] <- x
		currentChannelIndex++
	}

	//fmt.Println("all elements pushed in channel")
	for i := 0; i < numOfChannels; i++ {
		close(channels[i])
	}

	//fmt.Println("channels closed")
	waitGroup := sync.WaitGroup{}
	waitGroup.Add(numOfChannels)

	outTemp := make(chan int, ConstChannelBuffer)
	for i := 0; i < numOfChannels; i++ {

		// i was getting incremented before it being accessed by go block below. hence created this local variable
		iCopy := i

		go func() {
			defer waitGroup.Done()
			sumWorker(channels[iCopy], outTemp)
		}()
	}

	waitGroup.Wait()
	//fmt.Println("Triggering final merge")
	close(outTemp)

	out := make(chan int, ConstChannelBuffer)

	waitGroup = sync.WaitGroup{}
	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()
		sumWorker(outTemp, out)
	}()

	waitGroup.Wait()
	return <-out
}

// Read a list of integers separated by whitespace from `r`.
// Return the integers successfully read with no error, or
// an empty slice of integers and the error that occurred.
// Do NOT modify this function.
func readInts(r io.Reader) ([]int, error) {
	scanner := bufio.NewScanner(r)
	scanner.Split(bufio.ScanWords)
	var elems []int
	for scanner.Scan() {
		val, err := strconv.Atoi(scanner.Text())
		if err != nil {
			return elems, err
		}
		elems = append(elems, val)
	}
	return elems, nil
}
