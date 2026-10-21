package models

import (
	"testing"

	"github.com/spf13/viper"
)

func TestConvIdOfNormalization(t *testing.T) {
	if got := convIdOf(1, 9, 3); got != "p_3_9" {
		t.Fatalf("private must normalize small_first, got %s", got)
	}
	if got := convIdOf(1, 3, 9); got != "p_3_9" {
		t.Fatalf("private must be order-insensitive, got %s", got)
	}
	if got := convIdOf(2, 9, 42); got != "g_42" {
		t.Fatalf("group conv is g_{target}, got %s", got)
	}
	if got := ConvIdPrivate(7, 7); got != "p_7_7" {
		t.Fatalf("self conv legal (发给自己), got %s", got)
	}
}

func TestHbKeyDeviceIsolation(t *testing.T) {
	a := hbKey(&Node{UserId: 5, DeviceId: "devA"})
	b := hbKey(&Node{UserId: 5, DeviceId: "devB"})
	if a == b {
		t.Fatal("same user different devices MUST NOT share one timewheel key")
	}
	if a != "hb:5#devA" {
		t.Fatalf("unexpected format: %s", a)
	}
}

func TestConvPeer(t *testing.T) {
	if p := convPeer("p_3_9", 3); p != 9 {
		t.Fatalf("peer of 3 in p_3_9 want 9, got %d", p)
	}
	if p := convPeer("p_3_9", 9); p != 3 {
		t.Fatalf("peer of 9 want 3, got %d", p)
	}
	if p := convPeer("p_3_9", 4); p != 0 {
		t.Fatalf("non-member peer must be 0, got %d", p)
	}
	if p := convPeer("g_7", 3); p != 0 {
		t.Fatalf("group has no single peer, got %d", p)
	}
}

func TestParseMemberIdsSentinel(t *testing.T) {
	ids := parseMemberIds([]string{"11", "0", "22"}) // 负缓存哨兵 "0" 不得混进成员表
	if len(ids) != 2 || ids[0] != 11 || ids[1] != 22 {
		t.Fatalf("sentinel not filtered: %v", ids)
	}
}

func TestGroupPushThreshold(t *testing.T) {
	viper.Set("push.groupPushMaxMembers", 4)
	if got := groupPushThreshold(); got != 4 {
		t.Fatalf("env-configurable threshold broken: %d", got)
	}
	viper.Set("push.groupPushMaxMembers", 0)
	if got := groupPushThreshold(); got != 200 {
		t.Fatalf("default threshold want 200, got %d", got)
	}
	viper.Set("push.groupPushMaxMembers", 200) // 复位
}
