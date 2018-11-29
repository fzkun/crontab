package worker

import (
	"context"
	"github.com/coreos/etcd/clientv3"
	"github.com/fzkun/crontab/common"
	"log"
)

//分布式锁（TXN事务）
type JobLock struct {
	kv    clientv3.KV
	lease clientv3.Lease

	jobName    string             //任务名
	cancelFunc context.CancelFunc //用于终止自动续约
	leaseId    clientv3.LeaseID   //租约ID
	isLocked   bool               //是否上锁成功
}

//初始化一把锁
func InitJobLock(jobName string, kv clientv3.KV, lease clientv3.Lease) (jobLock *JobLock) {
	jobLock = &JobLock{
		kv:      kv,
		lease:   lease,
		jobName: jobName,
	}

	return
}

func (jobLock *JobLock) TryLock() (err error) {
	var (
		leaseGrantResp *clientv3.LeaseGrantResponse
		cancelCtx      context.Context
		cancleFunc     context.CancelFunc
		leaseId        clientv3.LeaseID
		keepRespChan   <-chan *clientv3.LeaseKeepAliveResponse
		txn            clientv3.Txn
		txnResp        *clientv3.TxnResponse
		lockKey        string
	)
	//1.创建租约5s
	if leaseGrantResp, err = jobLock.lease.Grant(context.TODO(), 5); err != nil {
		return
	}

	cancelCtx, cancleFunc = context.WithCancel(context.TODO())
	leaseId = leaseGrantResp.ID
	//2.自动续租
	if keepRespChan, err = jobLock.lease.KeepAlive(cancelCtx, leaseId); err != nil {
		goto FAIL
	}

	//3.处理续租应答的协程
	go func() {
		for {
			var (
				keepResp *clientv3.LeaseKeepAliveResponse
			)
			select {

			case keepResp = <-keepRespChan: //自动续租的应答
				if keepResp == nil {
					goto END
				}

			}
		}
	END:
	}()
	//4.创建事务txn
	txn = jobLock.kv.Txn(context.TODO())
	//锁路径
	lockKey = common.JOB_LOCK_DIR + jobLock.jobName

	//5.事务抢锁
	txn.If(clientv3.Compare(clientv3.CreateRevision(lockKey), "=", 0)).
		Then(clientv3.OpPut(lockKey, "", clientv3.WithLease(leaseId))).
		Else(clientv3.OpGet(lockKey))
	//提交事务
	if txnResp, err = txn.Commit(); err != nil {
		goto FAIL
	}

	//6.成功返回/失败释放租约
	if !txnResp.Succeeded { //锁被占用
		err = common.ERR_LOCK_ALREADY_REQUIRED
		goto FAIL
	}

	//抢锁成功
	jobLock.leaseId = leaseId
	jobLock.cancelFunc = cancleFunc
	jobLock.isLocked = true
	log.Println("抢锁成功:", jobLock.jobName)

	return

FAIL:
	cancleFunc()                                  //取消自动续租
	jobLock.lease.Revoke(context.TODO(), leaseId) //释放租约
	return
}

func (jobLock *JobLock) UnLock() {
	if jobLock.isLocked {
		jobLock.cancelFunc()                                  //取消程序自动续租协程
		jobLock.lease.Revoke(context.TODO(), jobLock.leaseId) //释放租约
	}

}
