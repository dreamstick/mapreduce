package mapreduce

import (
	"os"
	"encoding/json"
	"sort"
	"log"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	keyValues := make(map[string][]string)

	//从每个doMap生成的输出文件中获取属于该reduce函数的中间文件
	for i := 0; i < nMap; i++ {
		fileName := reduceName(jobName, i, reduceTask)
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatal("ERROR[doReduce]: Open error: ", err)
		}
		dec := json.NewDecoder(file)

		//将文件中的内容读如keyValues
		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				break
			}
			_, ok := keyValues[kv.Key]
			if !ok {
				keyValues[kv.Key] = make([]string, 0)
			}
			keyValues[kv.Key] = append(keyValues[kv.Key], kv.Value)
		}
		file.Close()
	}

	//按照key排序，并且合并中间文件
	var keys []string
	for k := range keyValues {
		keys = append(keys, k)
	}

	sort.Strings(keys)
	mergeFileName := mergeName(jobName, reduceTask)
	mergeFile, err := os.Create(mergeFileName)
	if err != nil {
		log.Fatal("ERROR[doReduce]: Create file error: ", err)
	}
	enc := json.NewEncoder(mergeFile)
	for _, k := range keys {
		res := reduceF(k, keyValues[k])
		enc.Encode(&KeyValue{k, res})
	}
	mergeFile.Close()
}
