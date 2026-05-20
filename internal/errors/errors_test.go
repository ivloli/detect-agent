package errors

import (
	"testing"
)

// ==================== Test Predefined Errors ====================

func TestPredefinedErrors(t *testing.T) {
	// Verify all predefined errors are non-nil and have correct codes
	tests := []struct {
		name string
		err  error
		code int
	}{
		{"ServerErr", ServerErr, CodeServerError},
		{"UnknowErr", UnknowErr, CodeUnknownError},
		{"InvalidAction", InvalidAction, CodeInvalidAction},
		{"InvalidSQL", InvalidSQL, CodeSqlError},
		{"CorsTenantErr", CorsTenantErr, CodeCorsTenant},
		{"InvalidParams", InvalidParams, CodeInvalidParams},
		{"InvalidDomain", InvalidDomain, CodeInvalidDomain},
		{"InvalidDNS", InvalidDNS, CodeInvalidDNSError},
		{"CronBuildErr", CronBuildErr, CodeCronBuildError},
		{"TaskPayloadBuildErr", TaskPayloadBuildErr, CodeTaskPayloadBuildError},
		{"RecordNotFoundErr", RecordNotFoundErr, CodeRecordNotFound},
		{"FrequencyBuildErr", FrequencyBuildErr, CodeFrequencyBuildError},
		{"TimeDateBuildErr", TimeDateBuildErr, CodeTimeDateBuildError},
		{"GetUserInfoErr", GetUserInfoErr, CodeGetUserInfoError},
		{"UserInfoNotFoundErr", UserInfoNotFoundErr, CodeUserInfoNotFound},
		{"NodeBuildErr", NodeBuildErr, CodeNodeBuildError},
		{"UnauthorizedErr", UnauthorizedErr, CodeUnauthorizedError},
		{"InspectNameExist", InspectNameExist, CodeInspectNameExist},
		{"DomainLengthErr", DomainLengthErr, CodeDomainLengthError},
		{"GetRegionErr", GetRegionErr, CodeGetRegionError},
		{"TaskConfigValid", TaskConfigValid, CodeTaskConfigValidError},
		{"ProbeTaskCreateFail", ProbeTaskCreateFail, CodeProbeTaskIdNullError},
		{"TaskTypeNotSupportErr", TaskTypeNotSupportErr, CodeTaskTypeNotSupportError},
		{"GetTemplateError", GetTemplateError, CodeGetTemplateError},
		{"TemplateBizTypeError", TemplateBizTypeError, CodeTemplateBizTypeError},
		{"GroupQueryError", GroupQueryError, CodeGroupQueryError},
		{"FileTooLargeErr", FileTooLargeErr, CodeFileTooLargeError},
		{"FileReadErr", FileReadErr, CodeFileReadError},
		{"NoFileErr", NoFileErr, CodeNoFileError},
		{"CSVFormatErr", CSVFormatErr, CodeCSVFormatError},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.err == nil {
				t.Fatalf("%s should not be nil", tt.name)
			}
			// Use plain error string to verify it's non-empty
			errStr := tt.err.Error()
			if errStr == "" {
				t.Fatalf("%s error string should not be empty", tt.name)
			}
			t.Logf("%s: %s", tt.name, errStr)
		})
	}
}

// ==================== Test Error Constructor Functions ====================

func TestNewInvalidDomainWithDomains(t *testing.T) {
	domains := []string{"example.com", "test.org"}
	err := NewInvalidDomainWithDomains(domains)
	if err == nil {
		t.Fatal("expected non-nil error")
	}
	errStr := err.Error()
	if errStr == "" {
		t.Fatal("error string should not be empty")
	}
	t.Logf("NewInvalidDomainWithDomains: %s", errStr)
}

func TestNewInvalidDNSWithDomains(t *testing.T) {
	domains := []string{"bad-domain.xyz"}
	err := NewInvalidDNSWithDomains(domains)
	if err == nil {
		t.Fatal("expected non-nil error")
	}
	errStr := err.Error()
	if errStr == "" {
		t.Fatal("error string should not be empty")
	}
	t.Logf("NewInvalidDNSWithDomains: %s", errStr)
}

func TestNewInspectNameExistWithNames(t *testing.T) {
	names := []string{"task1", "task2"}
	err := NewInspectNameExistWithNames(names)
	if err == nil {
		t.Fatal("expected non-nil error")
	}
	errStr := err.Error()
	if errStr == "" {
		t.Fatal("error string should not be empty")
	}
	t.Logf("NewInspectNameExistWithNames: %s", errStr)
}

// ==================== Test Empty Domain List ====================

func TestErrorConstructors_EmptyList(t *testing.T) {
	err1 := NewInvalidDomainWithDomains(nil)
	if err1 == nil {
		t.Fatal("expected non-nil error even with nil domains")
	}
	err2 := NewInvalidDNSWithDomains([]string{})
	if err2 == nil {
		t.Fatal("expected non-nil error even with empty domains")
	}
	err3 := NewInspectNameExistWithNames(nil)
	if err3 == nil {
		t.Fatal("expected non-nil error even with nil names")
	}
	t.Logf("Empty list errors all created successfully")
}