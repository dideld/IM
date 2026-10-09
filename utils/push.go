package utils

// 离线推送挂点（野火阶段模型里的"推送阶段"预留位）：
//   Web IM 没有 APNs/FCM/厂商通道的凭证与常驻通知链路，本次只落"接口 + 挂点 + 开关"，
//   不接运营商服务。离线消息的到达补偿仍由"未读数 + unreadList/redisMsg 拉取"承担，
//   PushAdapter 只是未来多出来的一扇门（短信/邮件/厂商 push 各自注册实现即可，不改主链路）。
//   配置：push.enabled=true 且已注册适配器时才会真正触发。

import (
	"fmt"

	"github.com/spf13/viper"
)

// PushSummary 离线推送的最小摘要（不带正文全文，隐私与体积都友好）
type PushSummary struct {
	ConvId   string `json:"ConvId"`
	Seq      int64  `json:"Seq"`
	MsgId    int64  `json:"MsgId"`
	Sender   int64  `json:"Sender"`
	MsgType  int    `json:"MsgType"` //1私聊 2群聊
	Preview  string `json:"Preview"` // "你好" / "[图片]" / "[语音]"
}

type PushAdapter interface {
	OfflinePush(uid int64, s PushSummary) error
}

var pushAdapter PushAdapter

// RegisterPush 注册离线推送适配器（进程启动期调用，重复注册以最后一次为准）
func RegisterPush(a PushAdapter) { pushAdapter = a }

// OfflinePush 主链路唯一调用点。开关关或未注册适配器时静默 no-op——
// 注意 no-op ≠ 消息丢失：未读与拉取补偿不受影响，只是"多一条通知门"没开。
func OfflinePush(uid int64, s PushSummary) {
	if !viper.GetBool("push.enabled") || pushAdapter == nil {
		if viper.GetBool("push.debug") {
			fmt.Printf("[push-stub] offline uid=%d conv=%s seq=%d\n", uid, s.ConvId, s.Seq)
		}
		return
	}
	if err := pushAdapter.OfflinePush(uid, s); err != nil {
		fmt.Printf("OfflinePush failed uid=%d conv=%s: %v\n", uid, s.ConvId, err)
	}
}
