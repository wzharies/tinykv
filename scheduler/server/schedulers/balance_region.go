// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
	"sort"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

// Schedule 避免太多 region 堆积在一个 store
func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	//TODO  Your Code Here (3C).
	// 选择合适的store，根据region大小进行排序

	// 1. 选出所有的合适的Store，就是满足Up并且停机时间不超过maxDownTime
	stores := cluster.GetStores()
	if stores == nil || len(stores) < 1 {
		return nil
	}
	var suitStore []*core.StoreInfo
	for i := 0; i < len(stores); i++ {
		if stores[i].IsUp() && stores[i].DownTime() < cluster.GetMaxStoreDownTime() {
			suitStore = append(suitStore, stores[i])
		}
	}
	sort.Slice(suitStore, func(i, j int) bool {
		return suitStore[i].GetRegionSize() < suitStore[j].GetRegionSize()
	})

	// 2. 遍历 suitableStores，找到目标 region 和 store
	var sourceRegion *core.RegionInfo
	var sourceStore *core.StoreInfo
	for i := len(suitStore) - 1; i >= 0; i-- {
		item := suitStore[i]
		cluster.GetPendingRegionsWithLock(item.GetID(), func(container core.RegionsContainer) {
			sourceRegion = container.RandomRegion(nil, nil)
		})
		if sourceRegion != nil {
			sourceStore = item
			break
		}
		cluster.GetFollowersWithLock(item.GetID(), func(container core.RegionsContainer) {
			sourceRegion = container.RandomRegion(nil, nil)
		})
		if sourceRegion != nil {
			sourceStore = item
			break
		}
		cluster.GetLeadersWithLock(item.GetID(), func(container core.RegionsContainer) {
			sourceRegion = container.RandomRegion(nil, nil)
		})
		if sourceRegion != nil {
			sourceStore = item
			break
		}
	}
	if sourceRegion == nil {
		return nil
	}
	// 3. 判断目标 region 的 store 数量，如果小于 cluster.GetMaxReplicas 直接放弃本次操作
	if len(sourceRegion.GetStoreIds()) < cluster.GetMaxReplicas() {
		return nil
	}
	// 4. 再次从 suitableStores 里面找到一个目标 store，目标 store 不能在原来的 region 里面
	var targetStore *core.StoreInfo
	for i := 0; i < len(suitStore); i++ {
		if _, ok:=sourceRegion.GetStoreIds()[suitStore[i].GetID()];!ok{
			targetStore = suitStore[i];
			break
		}
	}
	if targetStore == nil {
		return nil
	}
	// 5. 判断两个 store 的 region size 差值是否小于 2*ApproximateSize，是的话放弃 region 移动
	if sourceStore.GetRegionSize()-targetStore.GetRegionSize() < sourceRegion.GetApproximateSize() {
		return nil
	}
	// 6. 创建 CreateMovePeerOperator 操作并返回
	peer, err := cluster.AllocPeer(targetStore.GetID())
	if err != nil {
		return nil
	}
	peerOperator, err := operator.CreateMovePeerOperator("move region", cluster, sourceRegion, operator.OpBalance, sourceStore.GetID(), targetStore.GetID(), peer.Id)
	if err != nil {
		return nil
	}
	return peerOperator

}
