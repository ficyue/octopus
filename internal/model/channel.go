package model

import (
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/bestruirui/octopus/internal/transformer/outbound"
)


// 全局轮询计数器（按渠道 ID 记录）
var roundRobinCounters = make(map[int]int)
var roundRobinMutex sync.Mutex

type AutoGroupType int

const (
	AutoGroupTypeNone  AutoGroupType = 0 //不自动分组
	AutoGroupTypeFuzzy AutoGroupType = 1 //模糊匹配
	AutoGroupTypeExact AutoGroupType = 2 //准确匹配
	AutoGroupTypeRegex AutoGroupType = 3 //正则匹配
)

type KeyMode int

const (
	KeyModeDefault    KeyMode = 0 // 默认(按分组配置)
	KeyModeRoundRobin KeyMode = 1 // 轮询
	KeyModeRandom     KeyMode = 2 // 随机
	KeyModeFailover   KeyMode = 3 // 故障转移
	KeyModeWeighted   KeyMode = 4 // 加权
)

type Channel struct {
	ID            int                   `json:"id" gorm:"primaryKey"`
	Name          string                `json:"name" gorm:"unique;not null"`
	Type          outbound.OutboundType `json:"type"`
	Enabled       bool                  `json:"enabled" gorm:"default:true"`
	BaseUrls      []BaseUrl             `json:"base_urls" gorm:"serializer:json"`
	Keys          []ChannelKey          `json:"keys" gorm:"foreignKey:ChannelID"`
	Model         string                `json:"model"`
	CustomModel   string                `json:"custom_model"`
	Proxy         bool                  `json:"proxy" gorm:"default:false"`
	Passthrough   bool                  `json:"passthrough" gorm:"default:false"`
	AutoSync      bool                  `json:"auto_sync" gorm:"default:false"`
	AutoGroup     AutoGroupType         `json:"auto_group" gorm:"default:0"`
	CustomHeader  []CustomHeader        `json:"custom_header" gorm:"serializer:json"`
	ParamOverride *string               `json:"param_override"`
	ChannelProxy  *string               `json:"channel_proxy"`
	Stats         *StatsChannel         `json:"stats,omitempty" gorm:"foreignKey:ChannelID"`
	MatchRegex    *string               `json:"match_regex"`
	KeyMode       KeyMode               `json:"key_mode" gorm:"default:0"`
}

type BaseUrl struct {
	URL   string `json:"url"`
	Delay int    `json:"delay"`
}

type CustomHeader struct {
	HeaderKey   string `json:"header_key"`
	HeaderValue string `json:"header_value"`
}

type ChannelKey struct {
	ID               int     `json:"id" gorm:"primaryKey"`
	ChannelID        int     `json:"channel_id"`
	Enabled          bool    `json:"enabled" gorm:"default:true"`
	ChannelKey       string  `json:"channel_key"`
	StatusCode       int     `json:"status_code"`
	LastUseTimeStamp int64   `json:"last_use_time_stamp"`
	TotalCost        float64 `json:"total_cost"`
	Remark           string  `json:"remark"`
	Priority         int     `json:"priority" gorm:"default:0"`
	FailCount        int     `json:"fail_count" gorm:"default:0"`
}

// ChannelUpdateRequest 渠道更新请求 - 仅包含变更的数据
type ChannelUpdateRequest struct {
	ID            int                    `json:"id" binding:"required"`
	Name          *string                `json:"name,omitempty"`
	Type          *outbound.OutboundType `json:"type,omitempty"`
	Enabled       *bool                  `json:"enabled,omitempty"`
	BaseUrls      *[]BaseUrl             `json:"base_urls,omitempty"`
	Model         *string                `json:"model,omitempty"`
	CustomModel   *string                `json:"custom_model,omitempty"`
	Proxy         *bool                  `json:"proxy,omitempty"`
	Passthrough   *bool                  `json:"passthrough,omitempty"`
	AutoSync      *bool                  `json:"auto_sync,omitempty"`
	AutoGroup     *AutoGroupType         `json:"auto_group,omitempty"`
	CustomHeader  *[]CustomHeader        `json:"custom_header,omitempty"`
	ChannelProxy  *string                `json:"channel_proxy,omitempty"`
	ParamOverride *string                `json:"param_override,omitempty"`
	MatchRegex    *string                `json:"match_regex,omitempty"`

	KeysToAdd    []ChannelKeyAddRequest    `json:"keys_to_add,omitempty"`
	KeysToUpdate []ChannelKeyUpdateRequest `json:"keys_to_update,omitempty"`
	KeysToDelete []int                     `json:"keys_to_delete,omitempty"`
}

type ChannelKeyAddRequest struct {
	Priority   int    `json:"priority"`
	Enabled    bool   `json:"enabled"`
	ChannelKey string `json:"channel_key" binding:"required"`
	Remark     string `json:"remark"`
}

type ChannelKeyUpdateRequest struct {
	Priority   *int   `json:"priority,omitempty"`
	ID         int     `json:"id" binding:"required"`
	Enabled    *bool   `json:"enabled,omitempty"`
	ChannelKey *string `json:"channel_key,omitempty"`
	Remark     *string `json:"remark,omitempty"`
}

// ChannelFetchModelRequest is used by /channel/fetch-model (not persisted).
type ChannelFetchModelRequest struct {
	Type    outbound.OutboundType `json:"type" binding:"required"`
	BaseURL string                `json:"base_url" binding:"required"`
	Key     string                `json:"key" binding:"required"`
	Proxy   bool                  `json:"proxy"`
}

func (c *Channel) GetBaseUrl() string {
	if c == nil || len(c.BaseUrls) == 0 {
		return ""
	}

	bestURL := ""
	bestDelay := 0
	bestSet := false

	for _, bu := range c.BaseUrls {
		if bu.URL == "" {
			continue
		}
		if !bestSet || bu.Delay < bestDelay {
			bestURL = bu.URL
			bestDelay = bu.Delay
			bestSet = true
		}
	}

	return bestURL
}

func (c *Channel) GetChannelKey() ChannelKey {
	keys := c.GetAvailableKeys()
	if len(keys) == 0 {
		return ChannelKey{}
	}
	return keys[0]
}

// GetAvailableKeys returns all available keys based on channel's KeyMode.
// Modes: 0=Default(priority+cost), 1=RoundRobin, 2=Random, 3=Failover(priority), 4=Weighted
func (c *Channel) GetAvailableKeys() []ChannelKey {
	if c == nil || len(c.Keys) == 0 {
		return nil
	}

	nowSec := time.Now().Unix()

	// Step 1: Filter available keys
	var available []ChannelKey
	for _, k := range c.Keys {
		if !k.Enabled || k.ChannelKey == "" {
			continue
		}
		if k.StatusCode == 429 && k.LastUseTimeStamp > 0 {
			if nowSec-k.LastUseTimeStamp < int64(5*time.Minute/time.Second) {
				continue
			}
		}
		available = append(available, k)
	}

	if len(available) == 0 {
		return nil
	}

	// Step 2: Sort/arrange based on KeyMode
	switch c.KeyMode {
	case KeyModeRoundRobin:
		// 轮询：按顺序循环
		roundRobinMutex.Lock()
		counter := roundRobinCounters[c.ID]
		roundRobinCounters[c.ID] = counter + 1
		roundRobinMutex.Unlock()
		
		// 按 ID 排序确保稳定顺序
		sort.Slice(available, func(i, j int) bool {
			return available[i].ID < available[j].ID
		})
		
		// 旋转切片，从当前位置开始
		if len(available) > 0 {
			idx := counter % len(available)
			available = append(available[idx:], available[:idx]...)
		}
		
	case KeyModeRandom:
		// 随机：打乱顺序
		rand.Shuffle(len(available), func(i, j int) {
			available[i], available[j] = available[j], available[i]
		})
		
	case KeyModeFailover:
		// 故障转移：按优先级排序（priority 越小越优先）
		sort.Slice(available, func(i, j int) bool {
			if available[i].Priority != available[j].Priority {
				return available[i].Priority < available[j].Priority
			}
			return available[i].TotalCost < available[j].TotalCost
		})
		
	case KeyModeWeighted:
		// 加权：按权重分配（weight 存储在 priority 字段，priority=0 表示默认权重 1）
		// 计算总权重
		totalWeight := 0
		for _, k := range available {
			w := k.Priority
			if w <= 0 {
				w = 1
			}
			totalWeight += w
		}
		
		if totalWeight > 0 {
			// 加权随机选择
			r := rand.Intn(totalWeight)
			cumulative := 0
			selectedIdx := 0
			for i, k := range available {
				w := k.Priority
				if w <= 0 {
					w = 1
				}
				cumulative += w
				if r < cumulative {
					selectedIdx = i
					break
				}
			}
			// 将选中的 key 放到第一位
			if selectedIdx > 0 {
				selected := available[selectedIdx]
				available = append(available[:selectedIdx], available[selectedIdx+1:]...)
				available = append([]ChannelKey{selected}, available...)
			}
		}
		
	default:
		// 默认：按 priority + cost 排序（原有逻辑）
		sort.Slice(available, func(i, j int) bool {
			if available[i].Priority != available[j].Priority {
				return available[i].Priority < available[j].Priority
			}
			return available[i].TotalCost < available[j].TotalCost
		})
	}

	return available
}
