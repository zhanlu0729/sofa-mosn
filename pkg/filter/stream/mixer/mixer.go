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

package mixer

import (
	"context"
	"sync/atomic"

	"github.com/alipay/sofa-mosn/pkg/api/v2"
	"github.com/alipay/sofa-mosn/pkg/config"
	"github.com/alipay/sofa-mosn/pkg/filter"
	"github.com/alipay/sofa-mosn/pkg/types"
)

func init() {
	filter.RegisterStream("mixer", CreateMixerFilterFactory)
}

type mixerFilter struct {
	context context.Context

	delayPercent  uint32
	delayDuration uint64
	delaying      uint32
	cb            types.StreamReceiverFilterCallbacks
}

func NewMixerFilter(context context.Context, config *v2.FaultInject) types.StreamReceiverFilter {
	return &mixerFilter{
		context:       context,
		delayPercent:  config.DelayPercent,
		delayDuration: config.DelayDuration,
	}
}

func (f *mixerFilter) OnDecodeHeaders(headers map[string]string, endStream bool) types.FilterHeadersStatus {
	if atomic.LoadUint32(&f.delaying) > 0 {
		return types.FilterHeadersStatusStopIteration
	}

	return types.FilterHeadersStatusContinue
}

func (f *mixerFilter) OnDecodeData(buf types.IoBuffer, endStream bool) types.FilterDataStatus {
	if atomic.LoadUint32(&f.delaying) > 0 {
		return types.FilterDataStatusStopIterationAndBuffer
	}

	return types.FilterDataStatusContinue
}

func (f *mixerFilter) OnDecodeTrailers(trailers map[string]string) types.FilterTrailersStatus {
	if atomic.LoadUint32(&f.delaying) > 0 {
		return types.FilterTrailersStatusStopIteration
	}

	return types.FilterTrailersStatusContinue
}

func (f *mixerFilter) SetDecoderFilterCallbacks(cb types.StreamReceiverFilterCallbacks) {
	f.cb = cb
}

func (f *mixerFilter) OnDestroy() {}

// ~~ factory
type FilterConfigFactory struct {
	FaultInject *v2.FaultInject
}

func (f *FilterConfigFactory) CreateFilterChain(context context.Context, callbacks types.StreamFilterChainFactoryCallbacks) {
	filter := NewMixerFilter(context, f.FaultInject)
	callbacks.AddStreamReceiverFilter(filter)
}

func CreateMixerFilterFactory(conf map[string]interface{}) (types.StreamFilterChainFactory, error) {
	return &FilterConfigFactory{
		FaultInject: config.ParseFaultInjectFilter(conf),
	}, nil
}
