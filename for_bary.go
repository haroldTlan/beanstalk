package main

import (
	//"bytes"
	"fmt"
	"github.com/99designs/cmdstalk/broker"
	"github.com/kr/beanstalk"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

const (
	address    = "127.0.0.1:11300"
	defaultTtr = 10 * time.Second
)

func main() {
	for {
		cmd := "weed upload --server 192.168.2.105:9333 --maxMB 64 ~/ansible.tar.gz"
		taskQueue(cmd)
		time.Sleep(2 * time.Second)
	}
}

func taskQueue(cmd string) {
	tube, id := queueJob("hello world", 10, defaultTtr) //jobname, urgency, ttr
	//expectStdout := []byte("HELLO WORLD")

	results := make(chan *broker.JobResult)
	b := broker.New(address, tube, 0, cmd, results)

	ticks := make(chan bool)
	defer close(ticks)
	go b.Run(ticks)
	ticks <- true // handle a single job

	result := <-results

	if result.JobId != id {
		fmt.Println("result.JobId %d != queueJob id %d", result.JobId, id)
	}
	/*if !bytes.Equal(result.Stdout, expectStdout) {
		fmt.Println("Stdout mismatch: '%s' != '%s'\n", string(result.Stdout), expectStdout)
	}*/

	if strings.Contains(string(result.Stdout), "error") {
		fmt.Println(strings.Split(string(result.Stdout), "error")[1])
	}
	if result.ExitStatus != 0 {
		fmt.Println("Unexpected exit status %d", result.ExitStatus)
	}

}

func queueJob(body string, priority uint32, ttr time.Duration) (string, uint64) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	tubeName := "cmdstalk-test-" + strconv.FormatInt(r.Int63(), 16)
	assertTubeEmpty(tubeName)

	c, err := beanstalk.Dial("tcp", address)
	if err != nil {
		fmt.Println(err)
	}

	tube := beanstalk.Tube{Conn: c, Name: tubeName}

	id, err := tube.Put([]byte(body), priority, 0, ttr)
	if err != nil {
		fmt.Println(err)
	}

	return tubeName, id
}
func assertTubeEmpty(tubeName string) {
	// TODO
}

func assertJobStat(id uint64, key, value string) {
	c, err := beanstalk.Dial("tcp", address)
	if err != nil {
		fmt.Println(err)

	}
	stats, err := c.StatsJob(id)
	if err != nil {
		fmt.Println(err)
	}
	if stats[key] != value {
		fmt.Println("job %d %s = %s, expected %s", id, key, stats[key], value)
	}
}
