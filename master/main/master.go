package main

import (
	"flag"
	"fmt"
	"github.com/fzkun/crontab/master"
	"runtime"
	"time"
)

var (
	confFile string //配置文件路径
)

func initArgs() {
	// master -config ./master.json
	// master -h
	flag.StringVar(&confFile, "config", "./master.json", "指定master.json")
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
	if err = master.InitConfig(confFile); err != nil {
		goto ERR
	}

	if err = master.InitJobMgr(); err != nil {
		goto ERR
	}

	// 启动api http服务
	if err = master.InitServer(); err != nil {
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
