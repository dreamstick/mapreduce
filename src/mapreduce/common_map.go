package mapreduce

import (
	"hash/fnv"
	"os"
	"encoding/json"
	"log"
)

func doMap(
	jobName string, // the name of the MapReduce job
	mapTask int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(filename string, contents string) []KeyValue,
) {
	file, err := os.Open(inFile)
	if err != nil {
		log.Fatal("ERROR[doMap]: Open file error ", err)
	}
	defer file.Close()

	// 获取文件状态信息
	fileInfo, err := file.Stat()
	if err != nil {
		log.Fatal("ERROR[doMap]: Get file state error ", err)
	}

	// 获取文件大小
	fileSize := fileInfo.Size()

	//读文件
	buffer := make([]byte, fileSize)
	_, err = file.Read(buffer)
	if err != nil {
		log.Fatal("ERROR[doMap]:Read error ", err)
	}

	// 处理文件内容
	middleRes := mapF(inFile, string(buffer))
	rSize := len(middleRes)

	// 生成中间文件
	//fileName由jobName、mapTask和reduceTask生成
	//使用 hash(kv.Key)%nReduce将contents分配给reduceTask
	for i := 0; i < nReduce; i++ {
		fileName := reduceName(jobName, mapTask, i)
		midFile, err := os.Create(fileName)
		if err != nil {
			log.Fatal("ERROR[doMap]: Create intermediate file fail ", err)
		}
		enc := json.NewEncoder(midFile)
		for r := 0; r < rSize; r++ {
			kv := middleRes[r]
			if ihash(kv.Key)%nReduce == i {
				err := enc.Encode(&kv)
				if err != nil {
					log.Fatal("ERROR[doMap]: Encode error: ", err)
				}
			}
		}
		midFile.Close()
	}
}

func ihash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}
