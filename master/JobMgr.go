package master

import (
	"context"
	"encoding/json"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/fzkun/crontab/master/common"
	"time"
)

// 任务管理器
type JobMgr struct {
	client *clientv3.Client
	kv     clientv3.KV
	lease  clientv3.Lease
}

var (
	// 单例
	G_jobMgr *JobMgr
)

// 初始化管理器
func InitJobMgr() (err error) {
	var (
		config clientv3.Config
		client *clientv3.Client
		kv     clientv3.KV
		lease  clientv3.Lease
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

	// 赋值单例
	G_jobMgr = &JobMgr{
		client: client,
		kv:     kv,
		lease:  lease,
	}
	return
}

func (jobMgr *JobMgr) SaveJob(job *common.Job) (oldJob *common.Job, err error) {
	// 把任务保存到/cron/jobs/任务名 -> json
	var (
		jobKey    string
		jobVale   []byte
		putResp   *clientv3.PutResponse
		oldJobObj common.Job
	)

	// etcd保存key
	jobKey = common.JOB_SAVE_DIR + job.Name
	//任务信息json
	if jobVale, err = json.Marshal(job); err != nil {
		return
	}
	//保存到etcd
	if putResp, err = jobMgr.kv.Put(context.TODO(), jobKey, string(jobVale), clientv3.WithPrevKV()); err != nil {
		return
	}

	if putResp.PrevKv != nil {
		if err = json.Unmarshal(putResp.PrevKv.Value, &oldJobObj); err != nil {
			err = nil
			return
		}
	}

	oldJob = &oldJobObj

	return
}

func (jobMgr *JobMgr) DeleteJob(name string) (oldJob *common.Job, err error) {
	var (
		jobKey     string
		deleteResp *clientv3.DeleteResponse
		oldJobObj  common.Job
	)

	jobKey = common.JOB_SAVE_DIR + name
	if deleteResp, err = jobMgr.kv.Delete(context.TODO(), jobKey, clientv3.WithPrevKV()); err != nil {
		return
	}

	if len(deleteResp.PrevKvs) != 0 {
		if err = json.Unmarshal(deleteResp.PrevKvs[0].Value, &oldJobObj); err != nil {
			err = nil
			return
		}
	}

	oldJob = &oldJobObj

	return
}

func (jobMgr *JobMgr) ListJob() (jobList []*common.Job, err error) {
	var (
		dirKey  string
		getResp *clientv3.GetResponse
		kvPair  *mvccpb.KeyValue
		job     *common.Job
	)

	dirKey = common.JOB_SAVE_DIR

	//保存到etcd
	if getResp, err = jobMgr.kv.Get(context.TODO(), dirKey, clientv3.WithPrefix()); err != nil {
		return
	}

	jobList = make([]*common.Job, 0)

	for _, kvPair = range getResp.Kvs {
		job = &common.Job{}
		if err = json.Unmarshal(kvPair.Value, job); err != nil {
			continue
		}
		jobList = append(jobList, job)
	}

	return
}

func (jobMgr *JobMgr) KillJob(name string) (err error) {
	var (
		killKey        string
		leaseGrantResp *clientv3.LeaseGrantResponse
		leaseId        clientv3.LeaseID
	)

	killKey = common.JOB_KILLER_DIR + name

	//worker监听put操作操作，创建一个租约让其稍后自动过期
	if leaseGrantResp, err = jobMgr.lease.Grant(context.TODO(), 1); err != nil {
		return
	}

	//租约ID
	leaseId = leaseGrantResp.ID
	//设置killer标志
	if _, err = jobMgr.kv.Put(context.TODO(), killKey, "", clientv3.WithLease(leaseId)); err != nil {
		return
	}

	//向/cron/killer/xxx

	return
}
