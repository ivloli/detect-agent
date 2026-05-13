package errors

import (
	"fmt"

	"github.com/go-kratos/kratos/v2/errors"
)

// 通用错误
var (
	ServerErr = errors.New(CodeServerError, commonFailReason, "服务错误")

	UnknowErr           = errors.New(CodeUnknownError, commonFailReason, "unknow error")
	InvalidAction       = errors.New(CodeInvalidAction, commonFailReason, "无效操作")
	InvalidSQL          = errors.New(CodeSqlError, commonFailReason, "查询错误")
	CorsTenantErr       = errors.New(CodeCorsTenant, commonFailReason, "不允许跨租户操作")
	InvalidParams       = errors.New(CodeInvalidParams, commonFailReason, "无效的参数")
	InvalidDomain       = errors.New(CodeInvalidDomain, commonFailReason, "无效的域名(e.g. example.com)")
	InvalidDNS          = errors.New(CodeInvalidDNSError, commonFailReason, "请输入已做了实际解析的域名地址")
	CronBuildErr        = errors.New(CodeCronBuildError, commonFailReason, "cron 构建失败")
	TaskPayloadBuildErr = errors.New(CodeTaskPayloadBuildError, commonFailReason, "task payload 构建失败")

	RecordNotFoundErr = errors.New(CodeRecordNotFound, commonFailReason, "结果不存在")
	FrequencyBuildErr = errors.New(CodeFrequencyBuildError, commonFailReason, "频率转换失败")
	TimeDateBuildErr  = errors.New(CodeTimeDateBuildError, commonFailReason, "时间格式转换错误")

	GetUserInfoErr      = errors.New(CodeGetUserInfoError, commonFailReason, "获取用户信息错误")
	UserInfoNotFoundErr = errors.New(CodeUserInfoNotFound, commonFailReason, "用户不存在")
	NodeBuildErr        = errors.New(CodeNodeBuildError, commonFailReason, "节点转换错误")

	UnauthorizedErr = errors.New(CodeUnauthorizedError, commonFailReason, "缺少鉴权信息")

	InspectNameExist = errors.New(CodeInspectNameExist, commonFailReason, "任务名称已经存在")

	DomainLengthErr = errors.New(CodeDomainLengthError, commonFailReason, "监控对象数量不符合要求")
	GetRegionErr    = errors.New(CodeGetRegionError, commonFailReason, "获取区域信息失败的")
	TaskConfigValid = errors.New(CodeTaskConfigValidError, commonFailReason, "任务配置不符合任务类型")

	ProbeTaskCreateFail   = errors.New(CodeProbeTaskIdNullError, commonFailReason, "拨测任务创建失败")
	TaskTypeNotSupportErr = errors.New(CodeTaskTypeNotSupportError, commonFailReason, "不支持的任务类型")

	GetTemplateError     = errors.New(CodeGetTemplateError, commonFailReason, "获取模板信息异常")
	TemplateBizTypeError = errors.New(CodeTemplateBizTypeError, commonFailReason, "模板业务类型错误")

	GroupQueryError = errors.New(CodeGroupQueryError, commonFailReason, "获取分组信息失败")

	// 文件导入相关错误
	FileTooLargeErr = errors.New(CodeFileTooLargeError, commonFailReason, "文件大小超过10MB限制")
	FileReadErr     = errors.New(CodeFileReadError, commonFailReason, "读取文件失败")
	NoFileErr       = errors.New(CodeNoFileError, commonFailReason, "未找到上传的文件，请确保使用 multipart/form-data 格式上传文件，字段名为 'file'")
	CSVFormatErr    = errors.New(CodeCSVFormatError, commonFailReason, "CSV文件格式错误")
)

// NewInvalidDomainWithDomains 创建包含无效域名的错误
func NewInvalidDomainWithDomains(domains []string) *errors.Error {
	return errors.New(CodeInvalidDomain, commonFailReason, fmt.Sprintf("无效的域名: %v", domains))
}

// NewInvalidDNSWithDomains 创建包含DNS解析失败域名的错误
func NewInvalidDNSWithDomains(domains []string) *errors.Error {
	return errors.New(CodeInvalidDNSError, commonFailReason, fmt.Sprintf("DNS解析失败的域名: %v", domains))
}

// NewInspectNameExistWithNames 创建包含重复任务名称的错误
func NewInspectNameExistWithNames(names []string) *errors.Error {
	return errors.New(CodeInspectNameExist, commonFailReason, fmt.Sprintf("任务名称已存在: %v", names))
}
