package main

import (
	"errors"
	"net/url"
	"strings"

	"github.com/alibaba/higress/plugins/wasm-go/pkg/wrapper"
	"github.com/tetratelabs/proxy-wasm-go-sdk/proxywasm"
	"github.com/tetratelabs/proxy-wasm-go-sdk/proxywasm/types"
	"github.com/tidwall/gjson"
)

const (
	secondNano              = 1000 * 1000 * 1000
	minuteNano              = 60 * secondNano
	hourNano                = 60 * minuteNano
	dayNano                 = 24 * hourNano
	tickMilliseconds uint32 = 500
)

type KeyRateLimitConfig struct {
	ruleId        int
	limitKeys     map[string]LimitItem
	limitByHeader string
	limitByParam  string
}

type LimitItem struct {
	key                   string
	tokensPerRefill       uint64
	refillIntervalNanosec uint64
	maxTokens             uint64
}

var ruleId int = 0
var setTick bool = false

var limits = make(map[int][]LimitItem)

func main() {
	wrapper.SetCtx(
		"key-rate-limit",
		wrapper.ParseConfigBy(parseConfig),
		wrapper.ProcessOntickBy(onTick),
		wrapper.ProcessRequestHeadersBy(onHttpRequestHeaders),
	)
}

func parseConfig(json gjson.Result, config *KeyRateLimitConfig, log wrapper.Log) error {
	//解析配置规则
	config.limitKeys = make(map[string]LimitItem)
	limitKeys := json.Get("limit_keys").Array()
	for _, item := range limitKeys {
		key := item.Get("key")
		if !key.Exists() || key.String() == "" {
			return errors.New("key name is required")
		}
		qps := item.Get("query_per_second")
		if qps.Exists() && qps.String() != "" {
			config.limitKeys[key.String()] = LimitItem{
				key:                   key.String(),
				tokensPerRefill:       qps.Uint(),
				refillIntervalNanosec: secondNano,
				maxTokens:             qps.Uint(),
			}
			continue
		}
		qpm := item.Get("query_per_minute")
		if qpm.Exists() && qpm.String() != "" {
			config.limitKeys[key.String()] = LimitItem{
				key:                   key.String(),
				tokensPerRefill:       qpm.Uint(),
				refillIntervalNanosec: minuteNano,
				maxTokens:             qpm.Uint(),
			}
			continue
		}
		qph := item.Get("query_per_hour")
		if qph.Exists() && qph.String() != "" {
			config.limitKeys[key.String()] = LimitItem{
				key:                   key.String(),
				tokensPerRefill:       qph.Uint(),
				refillIntervalNanosec: hourNano,
				maxTokens:             qph.Uint(),
			}
			continue
		}
		qpd := item.Get("query_per_day")
		if qpd.Exists() && qpd.String() != "" {
			config.limitKeys[key.String()] = LimitItem{
				key:                   key.String(),
				tokensPerRefill:       qpd.Uint(),
				refillIntervalNanosec: dayNano,
				maxTokens:             qpd.Uint(),
			}
			continue
		}
		return errors.New("one of 'query_per_second', 'query_per_minute', " +
			"'query_per_hour' or 'query_per_day' must be set")
	}
	if len(config.limitKeys) == 0 {
		return errors.New("no limit keys found in configuration")
	}
	limitByHeader := json.Get("limit_by_header")
	if limitByHeader.Exists() {
		config.limitByHeader = limitByHeader.String()
	}
	limitByParam := json.Get("limit_by_param")
	if limitByParam.Exists() {
		config.limitByParam = limitByParam.String()
	}
	emptyHeader := config.limitByHeader == ""
	emptyParam := config.limitByParam == ""
	if (emptyHeader && emptyParam) || (!emptyHeader && !emptyParam) {
		return errors.New("only one of 'limit_by_param' and 'limit_by_header' can be set")
	}
	curRuleId := ruleId
	config.ruleId = curRuleId
	ruleId += 1
	//利用解析配置规则进行令牌桶初始化
	for _, keyItem := range config.limitKeys {
		if _, ok := limits[curRuleId]; !ok {
			limits[curRuleId] = []LimitItem{keyItem}
		} else {
			limits[curRuleId] = append(limits[curRuleId], keyItem)
		}
		if !initializeTokenBucket(curRuleId, keyItem) {
			return errors.New("initialize tokenbucket fail")
		}
	}
	if !setTick {
		setTick = true
		err := proxywasm.SetTickPeriodMilliSeconds(tickMilliseconds)
		if err != nil {
			return errors.New("failed to set tick period")
		}
	}
	return nil
}

func onHttpRequestHeaders(ctx wrapper.HttpContext, config KeyRateLimitConfig, log wrapper.Log) types.Action {
	headerKey := config.limitByHeader
	paramKey := config.limitByParam
	var key string
	if headerKey != "" {
		header, err := proxywasm.GetHttpRequestHeader(headerKey)
		if err != nil {
			return types.ActionContinue
		}
		key = header
	} else {
		requestUrl, err := proxywasm.GetHttpRequestHeader(":path")
		if err != nil {
			return types.ActionContinue
		}
		if strings.Contains(requestUrl, paramKey) {
			param, parseErr := url.Parse(requestUrl)
			if parseErr != nil {
				return types.ActionContinue
			} else {
				params := param.Query()
				value, ok := params[paramKey]
				if ok && len(value) > 0 {
					key = value[0]
				}
			}
		}
	}
	limitKeys := config.limitKeys
	_, exists := limitKeys[key]
	if !exists {
		return types.ActionContinue
	}
	if !getToken(config.ruleId, key) {
		ctx.DontReadRequestBody()
		return tooManyRequest()
	}
	return types.ActionContinue
}

func tooManyRequest() types.Action {
	_ = proxywasm.SendHttpResponse(429, nil,
		[]byte("Too many requests,rate_limited\n"), -1)
	return types.ActionContinue
}

func onTick(config KeyRateLimitConfig, log wrapper.Log) {
	refillToken(limits)
}
