package errors

// code 码段规则：int类型，长度 8. xx (服务码段) + xx(模块码段) + xxxx(业务码段)
// 域名服务码段统一使用：20
// 模块码段按需定义，如：
//    ICP错误码段: 10
//    权限模块: 10 ~ 19
// 业务码段：0001 ~ 9999，自行定义不重复即可

const (
	commonFailReason = "fail"
	successReason    = "success"
)

// 通用错误码段
const (
	CodeServerError             = 20000001 // 服务错误
	CodeUnknownError            = 20000002 // 未知错误
	CodeUnsupportedType         = 20000003 // 不支持的类型
	CodeInvalidParams           = 20000004 // 参数无效
	CodeInvalidAction           = 20000005 // 无效操作
	CodeSqlError                = 20000006 // 数据查询错误
	CodeCorsTenant              = 20000007 // 跨租户操作
	CodeInvalidCtxParams        = 20000008 // ctx 参数无效
	CodeRecordNotFound          = 20000009 // 记录未找到
	CodeCronBuildError          = 20000010 // cron 构建失败
	CodeTaskPayloadBuildError   = 20000011 // task payload 构建失败
	CodeFrequencyBuildError     = 20000012 // 频率转换失败
	CodeTimeDateBuildError      = 20000013 // 时间格式转换错误
	CodeGetUserInfoError        = 20000014 // 获取用户信息错误
	CodeUserInfoNotFound        = 20000015
	CodeNodeBuildError          = 20000016 // 节点转换错误
	CodeUnauthorizedError       = 20000017 // 缺少鉴权信息
	CodeInvalidDomain           = 20000018 // 无效的域名
	CodeInspectNameExist        = 20000019 // 任务名称已经存在
	CodeDomainLengthError       = 20000020 // 监控对象个数不符合要求
	CodeGetRegionError          = 20000021 // "获取区域信息失败的
	CodeTaskConfigValidError    = 20000022 // "任务配置不符合任务类型"
	CodeInvalidDNSError         = 20000023 // 请输入已做了实际解析的域名地址
	CodeProbeTaskIdNullError    = 20000024 // 拨测返回任务id为空(创建失败)
	CodeTaskTypeNotSupportError = 20000025 // 不支持的任务类型
	CodeFileTooLargeError       = 20000026 // 文件大小超过限制
	CodeFileReadError           = 20000027 // 文件读取失败
	CodeNoFileError             = 20000028 // 未找到上传的文件
	CodeCSVFormatError          = 20000029 // CSV文件格式错误
	CodeGetTemplateError        = 20000030 // 获取模板信息错误
	CodeTemplateBizTypeError    = 20000031 // 模板业务类型错误
	CodeGroupQueryError         = 20000032
)
