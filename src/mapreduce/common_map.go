package mapreduce

import (
	"encoding/json"
	"github.com1/traefik/log"
	"hash/fnv"
	"os"
)

func doMap(
	jobName string, // the name of the MapReduce job
	mapTask int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(filename string, contents string) []KeyValue,
) {
	//open inFile
	file, err := os.Open(inFile)
	if err != nil {
		log.Fatal("Error[doMap]:Open file error:", err)
	}
	defer file.Close()

	//获取文件状态信息
	fileInfo, err := file.Stat()
	if err != nil {
		log.Fatal("Error[doMap]:Get file stat error:", err)
	}

	//read file
	fileSize := fileInfo.Size()
	content := make([]byte, fileSize)
	_, err = file.Read(content)
	if err != nil {
		log.Fatal("Error[doMap]:Read error:", err)
	}

	//处理文件内容
	middleRes := mapF(inFile, string(content))
	rSize := len(middleRes)

	//生成nReduce个中间文件,使用hash(kv.Key) % nReduce 划分到不同的文件中， json格式
	for i := 0; i < nReduce; i++ {
		fileName := reduceName(jobName, mapTask, i)
		midFile, err := os.Create(fileName)
		if err != nil {
			log.Fatal("Error[doMap]:Create intermediate file error:", err)
		}
		enc := json.NewEncoder(midFile)
		for r := 0; r < rSize; r++ {
			kv := middleRes[r]
			if ihash(kv.Key)%nReduce == i {
				err := enc.Encode(&kv)
				if err != nil {
					log.Fatal("Error[doMap]:Encode error:", err)
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
