package cos418_hw1_1

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
)

// Sum numbers from channel `nums` and output sum to `out`.
// You should only output to `out` once.
// Do NOT modify function signature.
func sumWorker(nums chan int, out chan int) {
	// TODO: implement me
	// HINT: use for loop over `nums`
	sum := 0
	for num := range nums {
		sum += num
	}
	out <- sum
}

// Read integers from the file `fileName` and return sum of all values.
// This function must launch `num` go routines running
// `sumWorker` to find the sum of the values concurrently.
// You should use `checkError` to handle potential errors.
// Do NOT modify function signature.
func sum(num int, fileName string) int {
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
	for i := 0; i < num; i++ {
		channels = append(channels, make(chan int))
	}

	fmt.Println("Channel init done")

	// push elements in a cyclic fashion
	currentChannelIndex := 0
	for _, i := range nums {
		fmt.Println(i)
		channels[currentChannelIndex%num] <- i
		currentChannelIndex++
	}

	fmt.Println("elements pushed in channel")
	
	out := make(chan int)
	for i := 0; i < num; i++ {
		//close(channels[i])
		go sumWorker(channels[i], out)
		//go close(channels[i]) // close channel after work is done
	}

	fmt.Println("triggering final step")

	go sumWorker(out, out)
	return <-out
	return 0
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
