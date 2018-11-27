package worker

import "github.com/fzkun/crontab/master/common"

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

//协程调度
func (scheduler *Scheduler) ScheduleLop() {
	var (
		jobEvent *common.JobEvent
	)
	//定时任务commonJob
	for {
		select {
		case jobEvent = <-scheduler.jobEventChan: //监听任务变化事件
			//对内存中的任务列表做增删改查
			scheduler.handleJobEvent(jobEvent)
		}
	}

}

//推送任务变化事件
func (scheduler *Scheduler) PushJobEvent(jobEvent *common.JobEvent) {
	scheduler.jobEventChan <- jobEvent
}

/*初始化调度*/
func InitScheduler(err error) {
	G_scheduler = &Scheduler{
		jobEventChan: make(chan *common.JobEvent, 1000),
		jobPlanTable: make(map[string]*common.JobSchedulePlan),
	}
	go G_scheduler.ScheduleLop()
	return
}
