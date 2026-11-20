// Copyright 2026 TiKV Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package client

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/util/async"
)

type fakeLocalStore struct {
	addr        string
	storeID     uint64
	unavailable atomic.Bool
	calls       atomic.Int64
}

func (s *fakeLocalStore) IsLocal(addr string, storeID uint64) bool {
	return addr == s.addr || (storeID != 0 && storeID == s.storeID)
}

func (s *fakeLocalStore) Call(_ context.Context, req []byte, resp *tikvpb.BatchCommandsResponse_Response, _ time.Duration) error {
	if s.unavailable.Load() {
		return ErrLocalStoreUnavailable
	}
	s.calls.Add(1)
	decoded := &tikvpb.BatchCommandsRequest_Request{}
	if err := decoded.Unmarshal(req); err != nil {
		return err
	}
	get := decoded.GetGet()
	resp.Cmd = &tikvpb.BatchCommandsResponse_Response_Get{
		Get: &kvrpcpb.GetResponse{Value: append([]byte("v-"), get.GetKey()...)},
	}
	return nil
}

func newLocalGetRequest(storeID uint64) *tikvrpc.Request {
	req := tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{Key: []byte("k1"), Version: 10})
	req.Context.Peer = &metapb.Peer{StoreId: storeID}
	return req
}

func TestLocalStoreServesSyncRequest(t *testing.T) {
	store := &fakeLocalStore{addr: "127.0.0.1:1", storeID: 7}
	cli := NewRPCClient(WithLocalStore(store))
	defer cli.Close()

	resp, err := cli.SendRequest(context.Background(), "127.0.0.1:1", newLocalGetRequest(7), time.Second)
	require.NoError(t, err)
	require.Equal(t, []byte("v-k1"), resp.Resp.(*kvrpcpb.GetResponse).GetValue())
	require.EqualValues(t, 1, store.calls.Load())
}

func TestLocalStoreMatchesByStoreID(t *testing.T) {
	store := &fakeLocalStore{addr: "unused:1", storeID: 7}
	cli := NewRPCClient(WithLocalStore(store))
	defer cli.Close()

	resp, err := cli.SendRequest(context.Background(), "127.0.0.1:2", newLocalGetRequest(7), time.Second)
	require.NoError(t, err)
	require.Equal(t, []byte("v-k1"), resp.Resp.(*kvrpcpb.GetResponse).GetValue())
}

func TestLocalStoreSkipsForwardedAndTiFlash(t *testing.T) {
	store := &fakeLocalStore{addr: "127.0.0.1:1", storeID: 7}
	cli := NewRPCClient(WithLocalStore(store))
	defer cli.Close()

	forwarded := newLocalGetRequest(7)
	forwarded.ForwardedHost = "127.0.0.1:3"
	require.Nil(t, cli.localStoreFor("127.0.0.1:1", forwarded))

	tiflash := newLocalGetRequest(7)
	tiflash.StoreTp = tikvrpc.TiFlash
	require.Nil(t, cli.localStoreFor("127.0.0.1:1", tiflash))

	require.NotNil(t, cli.localStoreFor("127.0.0.1:1", newLocalGetRequest(7)))
}

func TestLocalStoreFallbackKeepsRequestEncoded(t *testing.T) {
	store := &fakeLocalStore{addr: "127.0.0.1:1", storeID: 7}
	store.unavailable.Store(true)

	batchReq := newLocalGetRequest(7).ToBatchCommandsRequest()
	_, fallback, err := sendLocalRequest(context.Background(), store, batchReq, tikvrpc.CmdGet, time.Second)
	require.NoError(t, err)
	require.True(t, fallback)
	encoded, ok := batchReq.Cmd.(*encodedBatchCmd)
	require.True(t, ok)
	require.NotNil(t, encoded.data, "fallback must leave the payload for the gRPC path")

	store.unavailable.Store(false)
	resp, fallback, err := sendLocalRequest(context.Background(), store, batchReq, tikvrpc.CmdGet, time.Second)
	require.NoError(t, err)
	require.False(t, fallback)
	require.Equal(t, []byte("v-k1"), resp.Resp.(*kvrpcpb.GetResponse).GetValue())
	require.Nil(t, encoded.data, "payload is returned to the pool after a local call")
}

func TestDefaultLocalStore(t *testing.T) {
	store := &fakeLocalStore{addr: "127.0.0.1:1", storeID: 7}
	SetDefaultLocalStore(store)
	defer SetDefaultLocalStore(nil)

	cli := NewRPCClient()
	defer cli.Close()
	require.NotNil(t, cli.localStoreFor("127.0.0.1:1", newLocalGetRequest(7)))

	// An explicit nil option disables the default.
	cli2 := NewRPCClient(WithLocalStore(nil))
	defer cli2.Close()
	require.Nil(t, cli2.localStoreFor("127.0.0.1:1", newLocalGetRequest(7)))
}

func TestLocalStoreServesAsyncRequest(t *testing.T) {
	store := &fakeLocalStore{addr: "127.0.0.1:1", storeID: 7}
	cli := NewRPCClient(WithLocalStore(store))
	defer cli.Close()

	rl := async.NewRunLoop()
	done := make(chan struct{})
	var (
		gotResp *tikvrpc.Response
		gotErr  error
	)
	cb := async.NewCallback(rl, func(resp *tikvrpc.Response, err error) {
		gotResp, gotErr = resp, err
		close(done)
	})
	cli.SendRequestAsync(context.Background(), "127.0.0.1:1", newLocalGetRequest(7), cb)
	for {
		select {
		case <-done:
			require.NoError(t, gotErr)
			require.Equal(t, []byte("v-k1"), gotResp.Resp.(*kvrpcpb.GetResponse).GetValue())
			return
		default:
			_, err := rl.Exec(context.Background())
			require.NoError(t, err)
		}
	}
}

// markPoolIdle creates the gRPC pool for addr and marks its batch connection
// idle, as the idle detector does after idleTimeout without gRPC traffic.
func markPoolIdle(t *testing.T, cli *RPCClient, addr string) {
	pool, err := cli.getConnPool(addr, true)
	require.NoError(t, err)
	atomic.StoreUint32(&pool.batchConn.idle, 1)
	_, err = cli.getConnPool(addr, true)
	require.ErrorContains(t, err, "rpcClient is idle")
}

// Local requests never use the batch connection, so it goes idle every
// idleTimeout under any load; that must not fail them.
func TestLocalStoreIgnoresIdleGRPCPool(t *testing.T) {
	store := &fakeLocalStore{addr: "127.0.0.1:1", storeID: 7}
	cli := NewRPCClient(WithLocalStore(store))
	defer cli.Close()
	markPoolIdle(t, cli, "127.0.0.1:1")

	resp, err := cli.SendRequest(context.Background(), "127.0.0.1:1", newLocalGetRequest(7), time.Second)
	require.NoError(t, err)
	require.Equal(t, []byte("v-k1"), resp.Resp.(*kvrpcpb.GetResponse).GetValue())

	rl := async.NewRunLoop()
	done := make(chan struct{})
	var (
		gotResp *tikvrpc.Response
		gotErr  error
	)
	cli.SendRequestAsync(context.Background(), "127.0.0.1:1", newLocalGetRequest(7), async.NewCallback(rl, func(resp *tikvrpc.Response, err error) {
		gotResp, gotErr = resp, err
		close(done)
	}))
	for {
		select {
		case <-done:
			require.NoError(t, gotErr)
			require.Equal(t, []byte("v-k1"), gotResp.Resp.(*kvrpcpb.GetResponse).GetValue())
			require.EqualValues(t, 2, store.calls.Load())
			return
		default:
			_, err := rl.Exec(context.Background())
			require.NoError(t, err)
		}
	}
}

// A local client that never falls back does not open gRPC connections to
// its own store.
func TestLocalStoreDoesNotDialGRPC(t *testing.T) {
	store := &fakeLocalStore{addr: "127.0.0.1:1", storeID: 7}
	cli := NewRPCClient(WithLocalStore(store))
	defer cli.Close()

	_, err := cli.SendRequest(context.Background(), "127.0.0.1:1", newLocalGetRequest(7), time.Second)
	require.NoError(t, err)
	cli.RLock()
	defer cli.RUnlock()
	require.Empty(t, cli.connPools)
}

type reportingLocalStore struct {
	fakeLocalStore
	asked atomic.Int64
}

func (s *reportingLocalStore) HealthFeedback() (*kvrpcpb.HealthFeedback, time.Duration, error) {
	n := s.asked.Add(1)
	return &kvrpcpb.HealthFeedback{StoreId: s.storeID, FeedbackSeqNo: uint64(n), SlowScore: 7}, time.Hour, nil
}

type feedbackRecorder struct {
	mu  sync.Mutex
	got []*kvrpcpb.HealthFeedback
}

func (r *feedbackRecorder) OnHealthFeedback(f *kvrpcpb.HealthFeedback) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.got = append(r.got, f)
}

// The local store's health feedback reaches the event listener, at most once
// per interval it asks for.
func TestLocalStoreHealthFeedback(t *testing.T) {
	store := &reportingLocalStore{fakeLocalStore: fakeLocalStore{addr: "127.0.0.1:1", storeID: 7}}
	cli := NewRPCClient(WithLocalStore(store))
	defer cli.Close()
	rec := &feedbackRecorder{}
	cli.SetEventListener(rec)

	for range 5 {
		_, err := cli.SendRequest(context.Background(), "127.0.0.1:1", newLocalGetRequest(7), time.Second)
		require.NoError(t, err)
	}
	require.EqualValues(t, 1, store.asked.Load())

	// An explicit request is answered at once.
	req := tikvrpc.NewRequest(tikvrpc.CmdGetHealthFeedback, &kvrpcpb.GetHealthFeedbackRequest{})
	req.Context.Peer = &metapb.Peer{StoreId: 7}
	_, err := cli.SendRequest(context.Background(), "127.0.0.1:1", req, time.Second)
	require.NoError(t, err)
	require.EqualValues(t, 2, store.asked.Load())

	rec.mu.Lock()
	defer rec.mu.Unlock()
	require.Len(t, rec.got, 2)
	require.EqualValues(t, 7, rec.got[0].GetSlowScore())
	require.EqualValues(t, 7, rec.got[0].GetStoreId())
}
