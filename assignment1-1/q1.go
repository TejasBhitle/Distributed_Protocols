package cos418_hw1_1

import (
	"fmt"
	"log"
	"os"
	"regexp"
	"sort"
	"strings"
)

// Find the top K most common words in a text document.
//
//	path: location of the document
//	numWords: number of words to return (i.e. k)
//	charThreshold: character threshold for whether a token qualifies as a word,
//		e.g. charThreshold = 5 means "apple" is a word but "pear" is not.
//
// Matching is case insensitive, e.g. "Orange" and "orange" is considered the same word.
// A word comprises alphanumeric characters only. All punctuation and other characters
// are removed, e.g. "don't" becomes "dont".
// You should use `checkError` to handle potential errors.
func topWords(path string, numWords int, charThreshold int) []WordCount {
	// TODO: implement me
	// HINT: You may find the `strings.Fields` and `strings.ToLower` functions helpful
	// HINT: To keep only alphanumeric characters, use the regex "[^0-9a-zA-Z]+"

	// Step 1 : Reading file into string and then into words array
	byteArr, file_err := os.ReadFile(path)
	if file_err != nil {
		log.Fatal(file_err)
	}
	words := strings.Fields(string(byteArr))
	// fmt.Println(words)

	// Step 2: Filtering
	alphaNumericRegex, reg_error := regexp.Compile(`[^0-9a-zA-Z]+`)
	if reg_error != nil {
		log.Fatal(reg_error)
	}

	wordsMap := make(map[string]int)

	for _, word := range words {
		word = alphaNumericRegex.ReplaceAllString(word, "")
		if len(word) >= charThreshold {
			key := strings.ToLower(word)
			wordsMap[key]++
		}
	}

	wordCounts := []WordCount{}
	for key, value := range wordsMap {
		wordCounts = append(wordCounts, WordCount{Word: key, Count: value})
	}

	sortWordCounts(wordCounts)

	return wordCounts[:numWords] // getting the top k words
}

// A struct that represents how many times a word is observed in a document
type WordCount struct {
	Word  string
	Count int
}

func (wc WordCount) String() string {
	return fmt.Sprintf("%v: %v", wc.Word, wc.Count)
}

// Helper function to sort a list of word counts in place.
// This sorts by the count in decreasing order, breaking ties using the word.
// DO NOT MODIFY THIS FUNCTION!
func sortWordCounts(wordCounts []WordCount) {
	sort.Slice(wordCounts, func(i, j int) bool {
		wc1 := wordCounts[i]
		wc2 := wordCounts[j]
		if wc1.Count == wc2.Count {
			return wc1.Word < wc2.Word
		}
		return wc1.Count > wc2.Count
	})
}

func main() {
	topWords("simple.txt", 4, 0)
}
