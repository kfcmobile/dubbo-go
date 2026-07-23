/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package getty

import (
	"sync"
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
)

func TestClientTaskPoolReusesFirstPool(t *testing.T) {
	var manager clientTaskPool

	first, firstSize := manager.get(8)
	second, secondSize := manager.get(64)

	assert.Equal(t, first, second)
	assert.Equal(t, 8, firstSize)
	assert.Equal(t, firstSize, secondSize)
	first.Close()
}

func TestClientTaskPoolConcurrentInitialization(t *testing.T) {
	const callers = 64
	var (
		manager clientTaskPool
		wg      sync.WaitGroup
		pools   = make(chan interface{}, callers)
	)

	wg.Add(callers)
	for i := 0; i < callers; i++ {
		go func() {
			defer wg.Done()
			pool, _ := manager.get(32)
			pools <- pool
		}()
	}
	wg.Wait()
	close(pools)

	var first interface{}
	for pool := range pools {
		if first == nil {
			first = pool
			continue
		}
		assert.Equal(t, first, pool)
	}
	assert.Equal(t, 32, manager.size)
	manager.pool.Close()
}

func TestNormalizeClientGrpoolSize(t *testing.T) {
	assert.Equal(t, 16, normalizeClientGrpoolSize(16))
	assert.Greater(t, normalizeClientGrpoolSize(0), 0)
	assert.Greater(t, normalizeClientGrpoolSize(-1), 0)
}
