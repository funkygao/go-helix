package stages

import (
	"sync"
)

type ClusterDataCache struct {
	sync.RWMutex
}

func (c *ClusterDataCache) Refresh() {
	c.Lock()
	defer c.Unlock()

}
