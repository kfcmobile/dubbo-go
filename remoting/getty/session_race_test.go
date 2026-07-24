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
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

import (
	perrors "github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

func testClientSelectSessionRetry(t *testing.T, client *Client) {
	conn, err := client.pool.get()
	require.NoError(t, err)
	require.NotNil(t, conn)

	// Hold the session lock so the request pauses after selecting the client.
	conn.lock.Lock()
	if len(conn.sessions) == 0 {
		conn.lock.Unlock()
		t.Fatal("prepared client has no session")
	}
	activeMarker := time.Now().Unix() - 1
	conn.updateActive(activeMarker)

	type selectionResult struct {
		available bool
		err       error
	}
	resultCh := make(chan selectionResult, 1)
	go func() {
		_, session, selectErr := client.selectSession(client.addr)
		resultCh <- selectionResult{available: session != nil, err: selectErr}
	}()

	deadline := time.Now().Add(time.Second)
	for conn.getActive() == activeMarker && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if conn.getActive() == activeMarker {
		conn.lock.Unlock()
		t.Fatal("request did not select the prepared client")
	}

	// Reproduce the close callback removing the final session in the race window.
	conn.sessions = nil
	conn.lock.Unlock()

	result := <-resultCh
	require.NoError(t, result.err)
	require.True(t, result.available)
}

func TestClientPoolCoalescesConcurrentConnectionCreation(t *testing.T) {
	const callers = 64

	pool := newGettyRPCClientConnPool(&Client{}, 1, 5*time.Second)
	start := make(chan struct{})
	factoryStarted := make(chan struct{})
	releaseFactory := make(chan struct{})
	results := make(chan *gettyRPCClient, callers)
	errs := make(chan error, callers)

	var (
		factoryCalls int32
		startedOnce  sync.Once
		wg           sync.WaitGroup
	)
	factory := func(_ *gettyRPCClientPool, _ string) (*gettyRPCClient, error) {
		atomic.AddInt32(&factoryCalls, 1)
		startedOnce.Do(func() {
			close(factoryStarted)
		})
		<-releaseFactory
		return &gettyRPCClient{active: time.Now().Unix()}, nil
	}

	wg.Add(callers)
	for i := 0; i < callers; i++ {
		go func() {
			defer wg.Done()
			<-start
			conn, getErr := pool.getOrCreateGettyRPCClient("127.0.0.1:20000", factory)
			results <- conn
			errs <- getErr
		}()
	}

	close(start)
	<-factoryStarted
	close(releaseFactory)
	wg.Wait()
	close(results)
	close(errs)

	require.Equal(t, int32(1), atomic.LoadInt32(&factoryCalls))
	var first *gettyRPCClient
	for conn := range results {
		require.NotNil(t, conn)
		if first == nil {
			first = conn
			continue
		}
		require.Same(t, first, conn)
	}
	for getErr := range errs {
		require.NoError(t, getErr)
	}
	pool.close()
}

func TestClientPoolClosesConnectionCreatedDuringShutdown(t *testing.T) {
	pool := newGettyRPCClientConnPool(&Client{}, 1, 5*time.Second)
	factoryStarted := make(chan struct{})
	releaseFactory := make(chan struct{})
	resultCh := make(chan error, 1)
	created := &gettyRPCClient{active: time.Now().Unix()}

	factory := func(_ *gettyRPCClientPool, _ string) (*gettyRPCClient, error) {
		close(factoryStarted)
		<-releaseFactory
		return created, nil
	}
	go func() {
		_, getErr := pool.getOrCreateGettyRPCClient("127.0.0.1:20000", factory)
		resultCh <- getErr
	}()

	<-factoryStarted
	pool.close()
	close(releaseFactory)

	require.Equal(t, errClientPoolClosed, perrors.Cause(<-resultCh))
	require.Zero(t, created.getActive())
}
