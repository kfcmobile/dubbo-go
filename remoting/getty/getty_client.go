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
	"math/rand"
	"runtime"
	"sync"
	"time"
)

import (
	"github.com/apache/dubbo-getty"
	gxsync "github.com/dubbogo/gost/sync"
	perrors "github.com/pkg/errors"
	"gopkg.in/yaml.v2"
)

import (
	"github.com/apache/dubbo-go/common"
	"github.com/apache/dubbo-go/common/constant"
	"github.com/apache/dubbo-go/common/logger"
	"github.com/apache/dubbo-go/config"
	"github.com/apache/dubbo-go/remoting"
)

var (
	errSessionNotExist   = perrors.New("session not exist")
	errClientClosed      = perrors.New("client closed")
	errClientReadTimeout = perrors.New("maybe the client read timeout or fail to decode tcp stream in Writer.Write")

	clientConfMu sync.RWMutex
	clientConf   *ClientConfig

	processClientGrpool clientTaskPool
)

// clientTaskPool owns the process-wide task pool used by all Getty clients.
// The first valid client configuration fixes its size for the process lifetime.
type clientTaskPool struct {
	once sync.Once
	pool gxsync.GenericTaskPool
	size int
}

func normalizeClientGrpoolSize(size int) int {
	if size < 1 {
		return runtime.GOMAXPROCS(-1) * 100
	}
	return size
}

func (p *clientTaskPool) get(size int) (gxsync.GenericTaskPool, int) {
	size = normalizeClientGrpoolSize(size)
	p.once.Do(func() {
		p.size = size
		p.pool = gxsync.NewTaskPoolSimple(size)
	})
	return p.pool, p.size
}

// it is init client for single protocol.
func initClient(protocol string) (ClientConfig, gxsync.GenericTaskPool, error) {
	var emptyConfig ClientConfig
	if protocol == "" {
		return emptyConfig, nil, perrors.New("client protocol is empty")
	}

	// load clientconfig from consumer_config
	// default use dubbo
	consumerConfig := config.GetConsumerConfig()
	if consumerConfig.ApplicationConfig == nil {
		if configuredClient, ok := getClientConf(); ok {
			return configuredClient, setClientGrpool(configuredClient.GrPoolSize), nil
		}
		return emptyConfig, nil, perrors.New("consumer application config is nil")
	}
	protocolConf := consumerConfig.ProtocolConf
	defaultClientConfig := GetDefaultClientConfig()
	if protocolConf == nil {
		logger.Info("protocol_conf default use dubbo config")
	} else {
		protocolConfigs, ok := protocolConf.(map[interface{}]interface{})
		if !ok {
			return emptyConfig, nil, perrors.Errorf("invalid protocol_conf type %T", protocolConf)
		}
		dubboConf := protocolConfigs[protocol]
		if dubboConf == nil {
			return emptyConfig, nil, perrors.Errorf("client config for protocol %q is nil", protocol)
		}
		dubboConfByte, err := yaml.Marshal(dubboConf)
		if err != nil {
			return emptyConfig, nil, perrors.WithStack(err)
		}
		err = yaml.Unmarshal(dubboConfByte, &defaultClientConfig)
		if err != nil {
			return emptyConfig, nil, perrors.WithStack(err)
		}
	}
	if err := defaultClientConfig.CheckValidity(); err != nil {
		return emptyConfig, nil, perrors.WithStack(err)
	}
	setClientConf(defaultClientConfig)
	taskPool := setClientGrpool(defaultClientConfig.GrPoolSize)

	rand.Seed(time.Now().UnixNano())
	return defaultClientConfig, taskPool, nil
}

// Config ClientConf
func SetClientConf(c ClientConfig) {
	if err := c.CheckValidity(); err != nil {
		logger.Warnf("[ClientConfig CheckValidity] error: %v", err)
		return
	}
	setClientConf(c)
	setClientGrpool(c.GrPoolSize)
}

func setClientConf(c ClientConfig) {
	clientConfMu.Lock()
	clientConf = &c
	clientConfMu.Unlock()
}

func getClientConf() (ClientConfig, bool) {
	clientConfMu.RLock()
	defer clientConfMu.RUnlock()
	if clientConf == nil {
		return ClientConfig{}, false
	}
	return *clientConf, true
}

func setClientGrpool(size int) gxsync.GenericTaskPool {
	taskPool, configuredSize := processClientGrpool.get(size)
	requestedSize := normalizeClientGrpoolSize(size)
	if configuredSize != requestedSize {
		logger.Warnf("client goroutine pool already initialized with size %d; ignore requested size %d",
			configuredSize, requestedSize)
	}
	return taskPool
}

// Options : param config
type Options struct {
	// connect timeout
	// remove request timeout, it will be calculate for every request
	ConnectTimeout time.Duration
	// request timeout
	RequestTimeout time.Duration
}

// Client : some configuration for network communication.
type Client struct {
	addr           string
	opts           Options
	conf           ClientConfig
	mux            sync.RWMutex
	pool           *gettyRPCClientPool
	taskPool       gxsync.GenericTaskPool // Process-wide; Client.Close must not close it.
	codec          remoting.Codec
	ExchangeClient *remoting.ExchangeClient
}

// create client
func NewClient(opt Options) *Client {
	switch {
	case opt.ConnectTimeout == 0:
		opt.ConnectTimeout = 3 * time.Second
		fallthrough
	case opt.RequestTimeout == 0:
		opt.RequestTimeout = 3 * time.Second
	}

	c := &Client{
		opts: opt,
	}
	return c
}

func (c *Client) SetExchangeClient(client *remoting.ExchangeClient) {
	c.ExchangeClient = client
}

// init client and try to connection.
func (c *Client) Connect(url *common.URL) error {
	clientConfig, taskPool, err := initClient(url.Protocol)
	if err != nil {
		return perrors.WithStack(err)
	}
	c.conf = clientConfig
	c.taskPool = taskPool
	// new client
	c.pool = newGettyRPCClientConnPool(c, c.conf.PoolSize, time.Duration(int(time.Second)*c.conf.PoolTTL))
	c.pool.sslEnabled = url.GetParamBool(constant.SSL_ENABLED_KEY, false)

	// codec
	c.codec = remoting.GetCodec(url.Protocol)
	c.addr = url.Location
	_, _, err = c.selectSession(c.addr)
	if err != nil {
		logger.Errorf("try to connect server %v failed for : %v", url.Location, err)
	}
	return err
}

// close network connection
func (c *Client) Close() {
	c.mux.Lock()
	p := c.pool
	c.pool = nil
	c.mux.Unlock()
	if p != nil {
		p.close()
	}
}

// send request
func (c *Client) Request(request *remoting.Request, timeout time.Duration, response *remoting.PendingResponse) error {
	_, session, err := c.selectSession(c.addr)
	if err != nil {
		return perrors.WithStack(err)
	}
	if session == nil {
		return errSessionNotExist
	}
	var (
		totalLen int
		sendLen  int
	)
	if totalLen, sendLen, err = c.transfer(session, request, timeout); err != nil {
		if sendLen != 0 && totalLen != sendLen {
			logger.Warnf("start to close the session at request because %d of %d bytes data is sent success. err:%+v", sendLen, totalLen, err)
			go c.Close()
		}
		return perrors.WithStack(err)
	}

	if !request.TwoWay || response.Callback != nil {
		return nil
	}

	select {
	case <-getty.GetTimeWheel().After(timeout):
		return perrors.WithStack(errClientReadTimeout)
	case <-response.Done:
		err = response.Err
	}

	return perrors.WithStack(err)
}

// isAvailable returns true if the connection is available, or it can be re-established.
func (c *Client) IsAvailable() bool {
	client, _, err := c.selectSession(c.addr)
	return err == nil &&
		// defensive check
		client != nil
}

func (c *Client) selectSession(addr string) (*gettyRPCClient, getty.Session, error) {
	c.mux.RLock()
	defer c.mux.RUnlock()
	if c.pool == nil {
		return nil, nil, perrors.New("client pool have been closed")
	}
	rpcClient, err := c.pool.getGettyRpcClient(addr)
	if err != nil {
		return nil, nil, perrors.WithStack(err)
	}
	return rpcClient, rpcClient.selectSession(), nil
}

func (c *Client) transfer(session getty.Session, request *remoting.Request, timeout time.Duration) (int, int, error) {
	totalLen, sendLen, err := session.WritePkg(request, timeout)
	return totalLen, sendLen, perrors.WithStack(err)
}
