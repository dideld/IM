package utils

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"strings"
)

// Md5Encode 返回小写 MD5 摘要。
func Md5Encode(password string) string {
	h := md5.New()
	h.Write([]byte(password))
	return hex.EncodeToString(h.Sum(nil))
}

// MD5Encode 返回大写 MD5 摘要。
func MD5Encode(password string) string {
	return strings.ToUpper(Md5Encode(password))
}

// MakePassword 加盐哈希：Md5Encode(plain + salt)。
func MakePassword(plainpwd, salt string) string {
	return Md5Encode(plainpwd + salt)
}

// ValidPassword 校验密码是否匹配（比对加盐哈希）。
func ValidPassword(plainpwd, salt string, password string) bool {
	md := Md5Encode(plainpwd + salt)
	Info(context.Background(), "ValidPassword comparison", "hash", md, "match", md == password)
	return md == password
}