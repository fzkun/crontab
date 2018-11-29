package worker

import (
	"github.com/fzkun/crontab/common"
	"log"
	"time"
)

type Scheduler struct {
	jobEventChan     chan *common.JobEvent              //etcd任务事件队列
	jobPlanTable     map[string]*common.JobSchedulePlan //任务调度计划表
	jobExcutingTable map[string]*common.JobExecuteInfo  //正在执行任务表
	jobResultChan    chan *common.JobExecuteResult      //任务朱旭报告结果
}

var (
	G_scheduler *Scheduler
)

//处理任务事件
func (scheduler *Scheduler) handleJobEvent(jobEvent *common.JobEvent) {
	var (
		jobSchedulePlan *common.JobSchedulePlan
		jobExisted      bool
		err             error
	)
	switch jobEvent.EventType {
	case common.JOB_EVENT_SAVE: //保存任务
		if jobSchedulePlan, err = common.BuildJobSchedulePlan(jobEvent.Job); err != nil {
			return
		}
		scheduler.jobPlanTable[jobEvent.Job.Name] = jobSchedulePlan
	case common.JOB_EVENT_DELETE: //删除任务
		if jobSchedulePlan, jobExisted = scheduler.jobPlanTable[jobEvent.Job.Name]; jobExisted {
			delete(scheduler.jobPlanTable, jobEvent.Job.Name)
		}
	}
}

//尝试执行任务
func (scheduler *Scheduler) TryStartJob(jobPlan *common.JobSchedulePlan) {
	var (
		jobExecuteInfo *common.JobExecuteInfo
		jobExecuting   bool
	)
	//执行任务运行很久，但只执行一次，防止并发

	//如果正在执行，跳过本次
	if jobExecuteInfo, jobExecuting = scheduler.jobExcutingTable[jobPlan.Job.Name]; jobExecuting {
		log.Println(jobExecuteInfo.Job.Name, "任务执行中，跳过执行")
		return
	}

	//构建执行状态信息
	jobExecuteInfo = common.BuildJobExecuteInfo(jobPlan)
	//保存执行状态
	scheduler.jobExcutingTable[jobPlan.Job.Name] = jobExecuteInfo
	//TODO: 执行任务
	G_executor.ExecuteJob(jobExecuteInfo)
	//log.Println(jobExecuteInfo.Job.Name, ",", jobExecuteInfo.PlanTime, ",", jobExecuteInfo.RealTime)

	return
}

//重新计算任务调度状态
func (scheduler *Scheduler) TrySchedule() (scheduleAfter time.Duration) {
	var (
		jobPlan  *common.JobSchedulePlan
		now      time.Time
		nearTime *time.Time
	)

	//没有任务
	if len(scheduler.jobPlanTable) == 0 {
		scheduleAfter = 1 * time.Second
		return
	}

	//当前时间
	now = time.Now()

	// 1.遍历所有任务
	for _, jobPlan = range scheduler.jobPlanTable {
		// 2.过期任务立即执行
		if jobPlan.NextTime.Before(now) || jobPlan.NextTime.Equal(now) {
			//TODO:尝试执行任务
			scheduler.TryStartJob(jobPlan)
			jobPlan.NextTime = jobPlan.Expr.Next(now) //更新下次执行时间
		}

		//统计最近一个任务过期时间
		if nearTime == nil || jobPlan.NextTime.Before(*nearTime) {
			nearTime = &jobPlan.NextTime
		}
	}

	// 3.统计最近的要过期任务的事件（N秒后过期）最近时间-当前时间
	scheduleAfter = (*nearTime).Sub(now)
	return
}

//处理任务结果
func (scheduler *Scheduler) handleJobResult(result *common.JobExecuteResult) {
	//从执行表删除
	delete(scheduler.jobExcutingTable, result.ExecuteInfo.Job.Name)
}

//协程调度
func (scheduler *Scheduler) ScheduleLoop() {
	var (
		jobEvent      *common.JobEvent
		scheduleAfter time.Duration
		scheduleTimer *time.Timer
		jobResult     *common.JobExecuteResult
	)

	//初始化一次
	scheduleAfter = scheduler.TrySchedule()

	//调度定时器
	scheduleTimer = time.NewTimer(scheduleAfter)

	//定时任务commonJob
	for {
		select {
		case jobEvent = <-scheduler.jobEventChan: //监听任务变化事件
			//对内存中的任务列表做增删改查
			scheduler.handleJobEvent(jobEvent)
		case <-scheduleTimer.C: //最近的任务到期了
		case jobResult = <-scheduler.jobResultChan: //任务执行结果
			scheduler.handleJobResult(jobResult)
		}
		//调度一次任务
		scheduleAfter = scheduler.TrySchedule()
		//重置调度间隔
		scheduleTimer.Reset(scheduleAfter)
	}

}

//推送任务变化事件
func (scheduler *Scheduler) PushJobEvent(jobEvent *common.JobEvent) {
	scheduler.jobEventChan <- jobEvent
}

/*初始化调度*/
func InitScheduler() (err error) {
	G_scheduler = &Scheduler{
		jobEventChan:     make(chan *common.JobEvent, 1000),
		jobPlanTable:     make(map[string]*common.JobSchedulePlan),
		jobExcutingTable: make(map[string]*common.JobExecuteInfo),
		jobResultChan:    make(chan *common.JobExecuteResult, 1000),
	}
	go G_scheduler.ScheduleLoop()
	return
}

//回传任务执行结果
func (scheduler *Scheduler) PushJobResult(jobResult *common.JobExecuteResult) {
	scheduler.jobResultChan <- jobResult
}
