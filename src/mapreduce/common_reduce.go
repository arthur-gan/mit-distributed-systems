package mapreduce

import (
	"bufio"
	"encoding/json"
	"log"
	"os"
	"sort"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//

	// 	(1) Create key:list(value) map
	keyListVal := make(map[string][]string)

	//	(2) loop nMap
	//			interfileName
	//			file = os.open(interfileName)
	//			bufio.bufferedReader(file)
	//			dec json.NewDecoder
	//			for dec.more()
	for i := 0; i < nMap; i++ {
		interFileName := reduceName(jobName, i, reduceTask)
		intFile, err := os.Open(interFileName)
		if err != nil {
			log.Fatal(err)
		}
		intFileReader := bufio.NewReader(intFile)
		dec := json.NewDecoder(intFileReader)

		for dec.More() {
			var line KeyValue
			if err := dec.Decode(&line); err != nil {
				log.Println(err)
			}
			keyListVal[line.Key] = append(keyListVal[line.Key], line.Value)
		}

		intFile.Close()
	}

	//	(3) create output file and encoder
	outF, err := os.Create(outFile)
	if err != nil {
		log.Fatal(err)
	}
	bufW := bufio.NewWriter(outF)
	enc := json.NewEncoder(bufW)

	// 	(4) create sorted keys slice
	var sortedKeys []string
	for key := range keyListVal {
		sortedKeys = append(sortedKeys, key)
	}
	sort.Strings(sortedKeys)

	//	(5) loop sortedKeys
	//			enc.Encode(KeyValue{sortedKey, reduceF(keyListVal[sortedKey]))
	for _, sortedKey := range sortedKeys {
		enc.Encode(KeyValue{Key: sortedKey, Value: reduceF(sortedKey, keyListVal[sortedKey])})
	}

	bufW.Flush()
	outF.Close()
}
