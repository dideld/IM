package utils

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"strings"
)
//小写
func Md5Encode(password string)string{
	h:=md5.New()
	h.Write([]byte(password))
	tempStr:=h.Sum(nil)
	return hex.EncodeToString(tempStr)
}
//大写
func MD5Encode(password string)string{
	return strings.ToUpper(Md5Encode(password))
}

//加密
func MakePassword(plainpwd,salt string) string{
	return Md5Encode(plainpwd+salt)
}
//解密
func ValidPassword(plainpwd, salt string, password string) bool {
	md := Md5Encode(plainpwd + salt)
	Info(context.Background(), "ValidPassword comparison", "hash", md, "match", md == password)
	return md == password
}