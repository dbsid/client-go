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
	"runtime/trace"
	"sync/atomic"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/pkg/errors"
	"github.com/tikv/client-go/v2/internal/resourcecontrol"
	"github.com/tikv/client-go/v2/metrics"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/util"
	"github.com/tikv/client-go/v2/util/async"
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
	// buffer is only valid during the call. Call must return when ctx is done
	// or timeout (if positive) passes, as the gRPC path does, and must not
	// hold an OS thread while the store works: callers can have thousands of
	// requests in flight.
	Call(ctx context.Context, req []byte, resp *tikvpb.BatchCommandsResponse_Response, timeout time.Duration) error
}

// LocalHealthReporter is implemented by a LocalStore that reports the health
// feedback TiKV attaches to batch-commands responses over gRPC (slow score,
// noisy resource groups). The client asks for it at most once per returned
// interval while it sends local requests and passes it to the region cache,
// as it does for feedback that arrives on a gRPC stream.
type LocalHealthReporter interface {
	HealthFeedback() (feedback *kvrpcpb.HealthFeedback, interval time.Duration, err error)
}

// localFeedbackRetry is the wait before asking again when the store has no
// feedback (not ready, or health feedback disabled).
const localFeedbackRetry = 10 * time.Second

// maybeLocalHealthFeedback delivers the local store's health feedback when it
// is due, or at once for an explicit GetHealthFeedback request (as TiKV
// attaches it to the stream for one). One caller at a time fetches it; the
// others return at once.
func (c *RPCClient) maybeLocalHealthFeedback(store LocalStore, cmd tikvrpc.CmdType) {
	r, ok := store.(LocalHealthReporter)
	if !ok || c.option == nil {
		return
	}
	at := &c.option.localFeedbackAt
	now := time.Now().UnixNano()
	next := at.Load()
	if cmd != tikvrpc.CmdGetHealthFeedback &&
		(now < next || !at.CompareAndSwap(next, now+int64(localFeedbackRetry))) {
		return
	}
	feedback, interval, err := r.HealthFeedback()
	if err != nil || feedback == nil {
		return
	}
	if interval > 0 {
		at.Store(now + int64(interval))
	}
	if h := c.eventListener.Load(); h != nil {
		(*h).OnHealthFeedback(feedback)
	}
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

// sendRequestLocal serves req from the in-process store. ok is false when
// addr is not local or the store asked for fallback; the caller then sends req
// through gRPC.
//
// It runs before RPCClient.getConnPool: local requests never use the batch
// connection, so under any load its idle detector marks the pool idle every
// idleTimeout, and getConnPool then fails with "rpcClient is idle" until the
// pool is recycled. A failed primary commit is reported as undetermined and
// TiDB closes the client connection.
func (c *RPCClient) sendRequestLocal(
	ctx context.Context,
	spanRPC opentracing.Span,
	addr string,
	req *tikvrpc.Request,
	timeout time.Duration,
) (resp *tikvrpc.Response, ok bool, err error) {
	store := c.localStoreFor(addr, req)
	if store == nil {
		return nil, false, nil
	}
	batchReq := req.ToBatchCommandsRequest()
	if batchReq == nil {
		return nil, false, nil
	}
	start := time.Now()
	defer trace.StartRegion(ctx, req.Type.String()).End()
	resp, fallback, err := sendLocalRequest(ctx, store, batchReq, req.Type, timeout)
	if fallback {
		return nil, false, nil
	}
	c.maybeLocalHealthFeedback(store, req.Type)
	if resp != nil && !resourcecontrol.MakeRequestInfo(req).Bypass() {
		collectRUV2FromExecDetails(ctx, resp.GetExecDetailsV2())
	}
	if spanRPC != nil && util.TraceExecDetailsEnabled(ctx) {
		if si := buildSpanInfoFromResp(resp); si != nil {
			si.addTo(spanRPC, start)
		}
	}
	return resp, true, err
}

// sendRequestAsyncLocal is the asynchronous counterpart of sendRequestLocal:
// it reports false when addr is not local. Otherwise it serves the request on
// a new goroutine and, if the store asks for fallback, queues it on the gRPC
// batch connection. The callback mirrors the one in SendRequestAsync without
// the gRPC request metrics.
func (c *RPCClient) sendRequestAsyncLocal(
	ctx context.Context,
	addr string,
	req *tikvrpc.Request,
	batchReq *tikvpb.BatchCommandsRequest_Request,
	cb async.Callback[*tikvrpc.Response],
	regionRPC *trace.Region,
	spanRPC opentracing.Span,
	useCodec bool,
) bool {
	store := c.localStoreFor(addr, req)
	if store == nil {
		return false
	}
	if err := encodeRequestCmd(batchReq); err != nil {
		cb.Invoke(nil, err)
		return true
	}
	entry := &batchCommandsEntry{
		ctx:           ctx,
		req:           batchReq,
		cb:            cb,
		forwardedHost: req.ForwardedHost,
		pri:           req.GetResourceControlContext().GetOverridePriority(),
		reqArriveAt:   time.Now(),
	}
	var stop func() bool
	entry.cb.Inject(func(resp *tikvrpc.Response, err error) (*tikvrpc.Response, error) {
		if stop != nil {
			stop()
		}
		if resp != nil && !resourcecontrol.MakeRequestInfo(req).Bypass() {
			collectRUV2FromExecDetails(ctx, resp.GetExecDetailsV2())
		}
		if spanRPC != nil {
			if util.TraceExecDetailsEnabled(ctx) {
				if si := buildSpanInfoFromResp(resp); si != nil {
					si.addTo(spanRPC, entry.reqArriveAt)
				}
			}
			spanRPC.Finish()
		}
		regionRPC.End()
		if useCodec && err == nil {
			resp, err = c.option.codec.DecodeResponse(req, resp)
		}
		return resp, err
	})
	stop = context.AfterFunc(ctx, func() {
		entry.error(ctx.Err())
		atomic.StoreInt32(&entry.canceled, 1)
	})
	go func() {
		resp, fallback, err := callLocal(entry.ctx, store, entry.req, req.Type, 0)
		if fallback {
			c.queueBatchEntry(addr, entry)
			return
		}
		c.maybeLocalHealthFeedback(store, req.Type)
		if entry.isCanceled() {
			return
		}
		if err != nil {
			entry.error(err)
			return
		}
		entry.response(resp)
	}()
	return true
}

// queueBatchEntry sends an encoded entry through the gRPC batch connection
// to addr.
func (c *RPCClient) queueBatchEntry(addr string, entry *batchCommandsEntry) {
	connPool, err := c.getConnPool(addr, true)
	if err != nil {
		entry.error(err)
		return
	}
	select {
	case connPool.batchConn.batchCommandsCh <- entry:
	case <-entry.ctx.Done():
	case <-connPool.batchConn.closed:
		entry.error(errors.New("batchConn closed"))
	}
}
