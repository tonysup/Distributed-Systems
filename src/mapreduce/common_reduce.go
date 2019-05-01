package mapreduce

import (
	"os"
	"fmt"
	"encoding/json"
        "sort"
        "bufio"
        "io"
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
        fiOut, errOut := os.OpenFile(outFile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
        if errOut != nil {
          fmt.Printf("Error: %s\n", errOut)
          return
        }
        var kvarray []KeyValue
        for i := 0; i < nMap; i++ {
        reducefile := reduceName(jobName,i,reduceTask)
        fi, err := os.Open(reducefile)
  	    if err != nil {
      	   fmt.Printf("Error: %s\n", err)
           return
        }
        br := bufio.NewReader(fi)
        for {
          a, _, c := br.ReadLine()
          if c == io.EOF {
            break
          }
          
          var kv KeyValue
          json.Unmarshal(a, &kv)
          //fmt.Printf("key:%s,value:%s.\n",kv.Key,kv.Value)
          //fmt.Printf(string(a))
          //fmt.Printf("\n")
          kvarray = append(kvarray,kv) 
        }
        fi.Close()
        }
        sort.Slice(kvarray, func(i, j int) bool {
				  return kvarray[i].Key < kvarray[j].Key
			  })
        //fmt.Printf("kvArray size:%d\n",len(kvarray))
	      var tmpKey string
	      var first bool 
	      first = true
	      var tmpValue []string
	      for i := 0; i < len(kvarray); i++ {
           //fmt.Printf("tmpKey:%s,curKey:%s,first:%t.\n",tmpKey,kvarray[i].Key,first)
	         if tmpKey != kvarray[i].Key && !first {
			     values := reduceF(tmpKey,tmpValue)
			     kv := KeyValue{tmpKey, values}
			     b, err := json.Marshal(kv)
    	     if err != nil {
              fmt.Printf("Error: %s\n", err)
              return
           }
			     fiOut.Write(b)
		         fiOut.WriteString("\n")
		       //fmt.Printf("Write Ok.")
			     tmpValue = nil
			     tmpValue = append(tmpValue, kvarray[i].Value)
		       tmpKey=kvarray[i].Key
		       continue
	  }
	        tmpValue = append(tmpValue, kvarray[i].Value)
	        first = false
          tmpKey=kvarray[i].Key
	}
        values := reduceF(tmpKey,tmpValue)
        kv := KeyValue{tmpKey, values}
        b, err := json.Marshal(kv)
        if err != nil {
           fmt.Printf("Error: %s\n", err)
           return
        }
        fiOut.Write(b)
        fiOut.WriteString("\n")
        fiOut.Close()
}
