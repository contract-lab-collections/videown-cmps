package resp

import (
	"log"
	"runtime"

	"github.com/gin-gonic/gin"
	"github.com/pkg/errors"
)

const DEFAULT_ERROR_CODE = 10000
const DEFAULT_ERROR_HTTP_STATUS = 400
const DEFAULT_OK_HTTP_STATUS = 200

type stackTracer interface {
	StackTrace() errors.StackTrace
}

func logError(err error) {
	if err == nil {
		return
	}
	st, ok := errors.Cause(err).(stackTracer)
	if ok {
		log.Printf("[error] %+v\n", st)
	} else {
		pc, fn, line, _ := runtime.Caller(2)
		log.Printf("[error] in %s[%s:%d] %v\n", runtime.FuncForPC(pc).Name(), fn, line, err)
	}
}

func Error(c *gin.Context, err error) {
	logError(err)
	var r = errorResp{
		errorResult{
			Code: extractErrorCode(err),
			Msg:  err.Error(),
		},
	}
	c.JSON(extractHttpStatus(err, DEFAULT_ERROR_HTTP_STATUS), r)
}

func ErrorWithHttpStatus(c *gin.Context, err error, httpStatus int) {
	logError(err)
	var r = errorResp{
		errorResult{
			Code: extractErrorCode(err),
			Msg:  err.Error(),
		},
	}
	c.JSON(httpStatus, r)
}

func Ok(c *gin.Context, result any) {
	if result == nil {
		result = 1
	}
	c.JSON(extractHttpStatus(result, DEFAULT_OK_HTTP_STATUS), okResp{result})
}

func extractErrorCode(a any) int {
	var t, ok = a.(interface {
		ErrorCode() int
	})
	if ok {
		return t.ErrorCode()
	}
	return DEFAULT_ERROR_CODE
}

func extractHttpStatus(a any, d int) int {
	var t, ok = a.(interface {
		HttpStatusHint() int
	})
	if ok {
		return t.HttpStatusHint()
	}
	return d
}

type okResp struct {
	Ok any `json:"ok"`
}

type errorResult struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
}

type errorResp struct {
	Error errorResult `json:"error"`
}
