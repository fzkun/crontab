package main

import (
	"flag"
	"fmt"
	"github.com/fzkun/crontab/worker"
	"runtime"
	"time"
)

var (
	confFile string //配置文件路径
)

func initArgs() {
	// worker -config ./master.json
	// worker -h
	flag.StringVar(&confFile, "config", "./worker.json", "worker.json")
	flag.Parse()

}

func initEvn() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func main() {

	var (
		err error
	)

	initArgs()

	//初始化线程
	initEvn()

	//加载配置
	if err = worker.InitConfig(confFile); err != nil {
		goto ERR
	}

	if err = worker.InitJobMgr(); err != nil {
		goto ERR
	}

	//正常退出
	for {
		time.Sleep(1 * time.Second)
	}
	return

ERR:
	fmt.Println(err)
}
