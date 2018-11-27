package worker

import (
	"context"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/fzkun/crontab/master/common"
	"time"
)

// 任务管理器
type JobMgr struct {
	client  *clientv3.Client
	kv      clientv3.KV
	lease   clientv3.Lease
	watcher clientv3.Watcher
}

var (
	// 单例
	G_jobMgr *JobMgr
)

// 监听任务变化
func (jobMgr *JobMgr) watchJobs() (err error) {
	var (
		getResp            *clientv3.GetResponse
		kvpair             *mvccpb.KeyValue
		job                *common.Job
		watchStartRevision int64
		watchChan          clientv3.WatchChan
		watchResp          clientv3.WatchResponse
		watchEvent         *clientv3.Event
		jobName            string
		jobEvent           *common.JobEvent
	)
	// 1.get /cron/jobs/目录下所有任务，并获取当前集群revision
	if getResp, err = jobMgr.kv.Get(context.TODO(), common.JOB_SAVE_DIR, clientv3.WithPrefix()); err != nil {
		return
	}
	//当前任务
	for _, kvpair = range getResp.Kvs {
		if job, err = common.UnpackJob(kvpair.Value); err == nil {
			//TODO: job同步给schedule（调度协程）
			jobEvent = common.BuildJobEvent(common.JOB_EVENT_SAVE, job)
			fmt.Println(*jobEvent)
			G_scheduler.PushJobEvent(jobEvent)
		}

	}

	//2.从该revision开始监听
	go func() { //监听协程
		//下个版本开始监听
		watchStartRevision = getResp.Header.Revision + 1
		//监听/cron/jobs/目录后续变化
		watchChan = jobMgr.watcher.Watch(context.TODO(), common.JOB_SAVE_DIR, clientv3.WithRev(watchStartRevision), clientv3.WithPrefix())
		//处理监听事件
		for watchResp = range watchChan {
			for _, watchEvent = range watchResp.Events {
				switch watchEvent.Type {
				case mvccpb.PUT: //任务保存
					if job, err = common.UnpackJob(watchEvent.Kv.Value); err != nil {
						continue
					}
					//构造一个更新Event事件
					jobEvent = common.BuildJobEvent(common.JOB_EVENT_SAVE, job)

				case mvccpb.DELETE: //任务被删除
					jobName = common.ExtractJobName(string(watchEvent.Kv.Key))
					job = &common.Job{Name: jobName}
					//构造一个删除Event
					jobEvent = common.BuildJobEvent(common.JOB_EVENT_DELETE, job)

				}

				//TODO:推一个删除事件给schedule
				fmt.Println(*jobEvent)
				G_scheduler.PushJobEvent(jobEvent)
			}
		}
	}()

	return
}

// 初始化管理器
func InitJobMgr() (err error) {
	var (
		config  clientv3.Config
		client  *clientv3.Client
		kv      clientv3.KV
		lease   clientv3.Lease
		watcher clientv3.Watcher
	)

	// 初始化配置
	config = clientv3.Config{
		Endpoints:   G_config.EtcdEndpoints,                                     // 集群地址
		DialTimeout: time.Duration(G_config.EtcdDialTimeout) * time.Millisecond, // 连接超时
	}

	// 建立连接
	if client, err = clientv3.New(config); err != nil {
		return
	}

	// 得到KV和Lease的API子集
	kv = clientv3.NewKV(client)
	lease = clientv3.NewLease(client)
	watcher = clientv3.NewWatcher(client)

	// 赋值单例
	G_jobMgr = &JobMgr{
		client:  client,
		kv:      kv,
		lease:   lease,
		watcher: watcher,
	}

	//启动监听
	if err = G_jobMgr.watchJobs(); err != nil {
		return
	}
	return
}
