package utils

type ErrCode struct {
	Code    int
	Message string
}

var (
	OK              = ErrCode{0, "成功"}
	ErrInternal     = ErrCode{-1, "服务器内部错误"}
	ErrParam        = ErrCode{1001, "参数错误"}
	ErrUnauthorized = ErrCode{1002, "未登录或会话已过期"}
	ErrUserExists   = ErrCode{1003, "用户名已存在"}
	ErrUserNotFound = ErrCode{1004, "用户不存在"}
	ErrPassword     = ErrCode{1005, "密码错误"}
	ErrUpload       = ErrCode{2001, "上传失败"}
	ErrFileType     = ErrCode{2002, "文件类型不允许"}
	ErrMessage      = ErrCode{3001, "消息发送失败"}
)
