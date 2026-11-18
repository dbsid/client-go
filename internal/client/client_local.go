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
	"sync/atomic"
	"time"

	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/pkg/errors"
	"github.com/tikv/client-go/v2/metrics"
	"github.com/tikv/client-go/v2/tikvrpc"
)

// ErrLocalStoreUnavailable is returned by LocalStore.Call when the in-process
// store cannot serve the request (for example it is starting or stopping). The
// client then sends the request through gRPC instead.
var ErrLocalStoreUnavailable = errors.New("local store is unavailable")

// LocalStore is a TiKV store running in the same process as the client.
//
// When the target of a request is a local store, RPCClient skips gRPC and the
// batch-commands send/recv loops: the request is encoded once as a
// tikvpb.BatchCommandsRequest_Request and handed to Call, which executes it in
// process and decodes the tikvpb.BatchCommandsResponse_Response into resp.
// Codec, metrics, tracing and resource-control accounting stay identical to
// the gRPC path because the substitution happens below them.
type LocalStore interface {
	// IsLocal reports whether requests for storeID at addr are served in process.
	// storeID can be 0 if the caller does not know it.
	IsLocal(addr string, storeID uint64) bool
	// Call executes one encoded tikvpb.BatchCommandsRequest_Request. The req
	// buffer is only valid during the call.
	Call(ctx context.Context, req []byte, resp *tikvpb.BatchCommandsResponse_Response, timeout time.Duration) error
}

type localStoreHolder struct {
	store LocalStore
}

var defaultLocalStore atomic.Pointer[localStoreHolder]

// SetDefaultLocalStore installs the process-wide local store used by every
// RPCClient that is not created with WithLocalStore. It is meant for embedded
// deployments where exactly one TiKV runs inside the process. Passing nil
// removes it.
func SetDefaultLocalStore(store LocalStore) {
	if store == nil {
		defaultLocalStore.Store(nil)
		return
	}
	defaultLocalStore.Store(&localStoreHolder{store: store})
}

// WithLocalStore sets the in-process store of the client. It overrides the
// process-wide default.
func WithLocalStore(store LocalStore) Opt {
	return func(c *option) {
		c.localStore = store
		c.localStoreSet = true
	}
}

func (c *RPCClient) localStoreFor(addr string, req *tikvrpc.Request) LocalStore {
	if req.StoreTp != tikvrpc.TiKV || req.ForwardedHost != "" {
		return nil
	}
	var store LocalStore
	if c.option != nil && c.option.localStoreSet {
		store = c.option.localStore
	} else if h := defaultLocalStore.Load(); h != nil {
		store = h.store
	}
	if store == nil || !store.IsLocal(addr, req.Context.GetPeer().GetStoreId()) {
		return nil
	}
	return store
}

const (
	localResultOK          = "ok"
	localResultErr         = "err"
	localResultUnavailable = "fallback"
)

// callLocal executes req through the local store. When the store asks for
// fallback, req stays encoded and intact so the caller can send the same
// request through gRPC.
func callLocal(
	ctx context.Context,
	store LocalStore,
	req *tikvpb.BatchCommandsRequest_Request,
	cmdType tikvrpc.CmdType,
	timeout time.Duration,
) (resp *tikvpb.BatchCommandsResponse_Response, fallback bool, err error) {
	if err = encodeRequestCmd(req); err != nil {
		return nil, false, err
	}
	var payload []byte
	encoded, _ := req.Cmd.(*encodedBatchCmd)
	if encoded != nil {
		payload = *encoded.data
	}
	start := time.Now()
	resp = &tikvpb.BatchCommandsResponse_Response{}
	err = store.Call(ctx, payload, resp, timeout)
	label := cmdType.String()
	if errors.Is(err, ErrLocalStoreUnavailable) {
		metrics.TiKVLocalRequestCounter.WithLabelValues(label, localResultUnavailable).Inc()
		return nil, true, nil
	}
	if encoded != nil {
		globalEncodedMsgDataPool.Put(encoded.data)
		encoded.data = nil
	}
	if err != nil {
		metrics.TiKVLocalRequestCounter.WithLabelValues(label, localResultErr).Inc()
		return nil, false, err
	}
	metrics.TiKVLocalRequestCounter.WithLabelValues(label, localResultOK).Inc()
	metrics.TiKVLocalRequestDuration.WithLabelValues(label).Observe(time.Since(start).Seconds())
	return resp, false, nil
}

// sendLocalRequest is the synchronous counterpart of sendBatchRequest.
func sendLocalRequest(
	ctx context.Context,
	store LocalStore,
	req *tikvpb.BatchCommandsRequest_Request,
	cmdType tikvrpc.CmdType,
	timeout time.Duration,
) (*tikvrpc.Response, bool, error) {
	resp, fallback, err := callLocal(ctx, store, req, cmdType, timeout)
	if fallback || err != nil {
		return nil, fallback, err
	}
	r, err := tikvrpc.FromBatchCommandsResponse(resp)
	return r, false, err
}

// sendLocalEntryAsync serves an async batch entry through the local store on a
// new goroutine. On fallback it hands the untouched entry to the gRPC batch
// connection.
func sendLocalEntryAsync(store LocalStore, entry *batchCommandsEntry, cmdType tikvrpc.CmdType, batchConn *batchConn) {
	go func() {
		resp, fallback, err := callLocal(entry.ctx, store, entry.req, cmdType, 0)
		if fallback {
			select {
			case batchConn.batchCommandsCh <- entry:
			case <-entry.ctx.Done():
			case <-batchConn.closed:
				entry.error(errors.New("batchConn closed"))
			}
			return
		}
		if entry.isCanceled() {
			return
		}
		if err != nil {
			entry.error(err)
			return
		}
		entry.response(resp)
	}()
}
