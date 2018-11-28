package worker

import (
	"github.com/fzkun/crontab/master/common"
	"log"
	"time"
)

type Scheduler struct {
	jobEventChan chan *common.JobEvent              //etcd任务事件队列
	jobPlanTable map[string]*common.JobSchedulePlan //任务调度计划表
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
			log.Println("执行 job:", jobPlan.Job.Name)
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

//协程调度
func (scheduler *Scheduler) ScheduleLoop() {
	var (
		jobEvent      *common.JobEvent
		scheduleAfter time.Duration
		scheduleTimer *time.Timer
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
		jobEventChan: make(chan *common.JobEvent, 1000),
		jobPlanTable: make(map[string]*common.JobSchedulePlan),
	}
	go G_scheduler.ScheduleLoop()
	return
}
