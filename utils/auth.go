package utils

// 登录会话鉴权（Redis 会话 token）：
//   登录 → IssueToken 发随机 128bit token（login:token:{t}=userId，TTL 可配），
//   同时登记到 login:user:{uid} 集合，支持改密/封禁时整户踢线。
//   WS /chat 握手校验 ValidateToken，心跳 TouchToken 滑动续期。
//   token 自身带 TTL 自然过期，集合仅服务于批量撤销，过期无害。

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strconv"
	"time"

	"github.com/spf13/viper"
)

const (
	tokenKeyPrefix = "login:token:"
	userKeyPrefix  = "login:user:"
)

func tokenTTL() time.Duration {
	h := viper.GetInt("auth.tokenTTLHours")
	if h <= 0 {
		h = 24
	}
	return time.Duration(h) * time.Hour
}

// IssueToken 签发登录 token 并登记会话，返回 token（作为 user.identity 回传前端）
func IssueToken(userId uint) string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic("IssueToken crypto/rand failed: " + err.Error())
	}
	token := hex.EncodeToString(b)
	ctx := context.Background()
	pipe := Red.Pipeline()
	pipe.Set(ctx, tokenKeyPrefix+token, userId, tokenTTL())
	uk := userKeyPrefix + strconv.FormatUint(uint64(userId), 10)
	pipe.SAdd(ctx, uk, token)
	pipe.Expire(ctx, uk, tokenTTL())
	if _, err := pipe.Exec(ctx); err != nil {
		fmt.Println("IssueToken redis error:", err)
	}
	return token
}

// UserIdByToken 按 token 查会话主人（HTTP 层鉴权中间件用；不校验归属，返回值即身份）
func UserIdByToken(token string) (int64, bool) {
	if token == "" {
		return 0, false
	}
	uid, err := Red.Get(context.Background(), tokenKeyPrefix+token).Uint64()
	if err != nil {
		return 0, false
	}
	return int64(uid), true
}

// ValidateToken 校验 token 是否属于该用户（WS 握手使用，不续期）
func ValidateToken(token string, userId int64) bool {
	if token == "" || userId <= 0 {
		return false
	}
	uid, err := Red.Get(context.Background(), tokenKeyPrefix+token).Uint64()
	if err != nil {
		return false
	}
	return int64(uid) == userId
}

// TouchToken 心跳续期：token 与用户会话集合一并滑动过期
func TouchToken(token string, userId int64) {
	if token == "" {
		return
	}
	ctx := context.Background()
	Red.Expire(ctx, tokenKeyPrefix+token, tokenTTL())
	Red.Expire(ctx, userKeyPrefix+strconv.FormatInt(userId, 10), tokenTTL())
}

// DropUserSessions 踢线：删除该用户全部登录会话（改密等敏感更新时调用）
func DropUserSessions(userId uint) {
	ctx := context.Background()
	uk := userKeyPrefix + strconv.FormatUint(uint64(userId), 10)
	tokens, err := Red.SMembers(ctx, uk).Result()
	if err != nil {
		fmt.Println("DropUserSessions error:", err)
		return
	}
	keys := make([]string, 0, len(tokens)+1)
	keys = append(keys, uk)
	for _, t := range tokens {
		keys = append(keys, tokenKeyPrefix+t)
	}
	if len(keys) > 1 {
		if err := Red.Del(ctx, keys...).Err(); err != nil {
			fmt.Println("DropUserSessions del error:", err)
		}
	}
}
