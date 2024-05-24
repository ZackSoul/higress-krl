package main

import (
	"encoding/binary"
	"strconv"
	"time"

	"github.com/tetratelabs/proxy-wasm-go-sdk/proxywasm"
)

const (
	maxGetTokenRetry int = 20
)

// Key-prefix for token bucket shared data.
var tokenBucketPrefix string = "mse.token_bucket"

// Key-prefix for token bucket last updated time.
var lastRefilledPrefix string = "mse.last_refilled"

func getToken(ruleId int, key string) bool {
	tokenBucketKey := strconv.Itoa(ruleId) + tokenBucketPrefix + key
	for i := 0; i < maxGetTokenRetry; i++ {
		tokenBucketData, cas, err := proxywasm.GetSharedData(tokenBucketKey)
		if err != nil {
			continue
		}
		tokenLeft := binary.LittleEndian.Uint64(tokenBucketData)
		if tokenLeft == 0 {
			return false
		}
		tokenLeft -= 1
		tokenLeftBuf := make([]byte, 8)
		binary.LittleEndian.PutUint64(tokenLeftBuf, tokenLeft)
		err = proxywasm.SetSharedData(tokenBucketKey, tokenLeftBuf, cas)
		if err != nil {
			continue
		}
		return true
	}
	return true
}

func refillToken(rules map[int][]LimitItem) {
	for id, LimitItems := range rules {
		for _, rule := range LimitItems {
			lastRefilledKey := strconv.Itoa(id) + lastRefilledPrefix + rule.key
			tokenBucketKey := strconv.Itoa(id) + tokenBucketPrefix + rule.key
			last_update_data, last_update_cas, err := proxywasm.GetSharedData(lastRefilledKey)
			if err != nil {
				continue
			}
			last_update := binary.LittleEndian.Uint64(last_update_data)
			now := time.Now().UnixNano()
			if uint64(now)-last_update < rule.refillIntervalNanosec {
				continue
			}
			nowBuf := make([]byte, 8)
			binary.LittleEndian.PutUint64(nowBuf, uint64(now))
			err = proxywasm.SetSharedData(lastRefilledKey, nowBuf, last_update_cas)
			if err != nil {
				continue
			}
			for {
				last_update_data, last_update_cas, err = proxywasm.GetSharedData(tokenBucketKey)
				if err != nil {
					break
				}
				tokenLeft := binary.LittleEndian.Uint64(last_update_data)
				tokenLeft += rule.tokensPerRefill
				if tokenLeft > rule.maxTokens {
					tokenLeft = rule.maxTokens
				}
				tokenLeftBuf := make([]byte, 8)
				binary.LittleEndian.PutUint64(tokenLeftBuf, tokenLeft)
				err = proxywasm.SetSharedData(tokenBucketKey, tokenLeftBuf, last_update_cas)
				if err != nil {
					continue
				}
				break
			}
		}
	}
}

func initializeTokenBucket(ruleId int, rule LimitItem) bool {
	var initialValue uint64 = 0
	lastRefilledKey := strconv.Itoa(ruleId) + lastRefilledPrefix + rule.key
	tokenBucketKey := strconv.Itoa(ruleId) + tokenBucketPrefix + rule.key
	initialBuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(initialBuf, initialValue)
	maxTokenBuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(maxTokenBuf, rule.maxTokens)
	_, _, err := proxywasm.GetSharedData(lastRefilledKey)
	if err != nil {
		_ = proxywasm.SetSharedData(lastRefilledKey, initialBuf, 0)
		_ = proxywasm.SetSharedData(tokenBucketKey, maxTokenBuf, 0)
	} else {
		for {
			_, last_update_cas, err := proxywasm.GetSharedData(lastRefilledKey)
			if err != nil {
				return false
			}
			err = proxywasm.SetSharedData(lastRefilledKey, initialBuf, last_update_cas)
			if err != nil {
				continue
			}
			break
		}
		for {
			_, last_update_cas, err := proxywasm.GetSharedData(tokenBucketKey)
			if err != nil {
				return false
			}
			err = proxywasm.SetSharedData(tokenBucketKey, maxTokenBuf, last_update_cas)
			if err != nil {
				continue
			}
			break
		}
	}
	return true
}
