package worker

import (
	"context"
	"github.com/fzkun/crontab/master/common"
	"log"
	"os/exec"
	"time"
)

type Executor struct {
}

var (
	G_executor *Executor
)

func (executor *Executor) ExecuteJob(info *common.JobExecuteInfo) {
	go func() {
		var (
			cmd     *exec.Cmd
			err     error
			output  []byte
			result  *common.JobExecuteResult
			jobLock *JobLock
		)

		//任务结果
		result = &common.JobExecuteResult{
			ExecuteInfo: info,
			Output:      make([]byte, 0),
		}

		//初始化锁
		jobLock = G_jobMgr.CreateJobLock(info.Job.Name)

		//任务开始时间
		result.StartTime = time.Now()

		// 执行shell命令
		cmd = exec.CommandContext(context.TODO(), "/bin/bash", "-c", info.Job.Command)

		//执行并捕获输出
		output, err = cmd.CombinedOutput()
		result.Output = output

		//任务结束时间
		result.EndTime = time.Now()
		result.Err = err

		//任务执行完成后，把执行结果返回给Scheduler，Scheduler会从executingTable删除执行记录
		log.Println("正在执行:", info.Job.Name, "result:", string(output))
		G_scheduler.PushJobResult(result)
	}()
}

func InitExecutor() (err error) {
	G_executor = &Executor{}
	return
}
