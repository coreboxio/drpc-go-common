package logging

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/coreboxio/drpc-go-common/utils"
)

const (
	DEBUG = iota
	INFO
	WARN
	ERROR
	FATAL
)

var (
	logLevel         = INFO
	executableName   = ""
	pid              = 0
	logTag           = ""
	levelStr         = "INFO"
	ip               = ""
	logEndpoint      = "std::cout"
	dlog             *DLog
	logBodyMaxLength = 1024
)

func Init(tag string, execName string, logEndpoint string, logLevel string, ifaName string) {
	SetExecutableName(execName)
	SetLogLevel(logLevel)
	SetIfaName(ifaName)
	SetLogEndpoint(logEndpoint)
	logTag = tag
	if strings.HasPrefix(logEndpoint, "unix://") {
		dlog = NewDLog(strings.TrimPrefix(logEndpoint, "unix://"), 3*time.Second, 1)
	} else {
		dlog = nil
	}
}

func Destroy() {
	if strings.HasPrefix(logEndpoint, "unix://") {
		dlog.Destroy()
	}
}

func SetLogEndpoint(endpoint string) {
	logEndpoint = endpoint
}

func SetLogLevel(lstr string) {
	levelStr = strings.ToUpper(lstr)
	switch levelStr {
	case "DEBUG":
		logLevel = DEBUG
	case "INFO":
		logLevel = INFO
	case "WARN":
		logLevel = WARN
	case "ERROR":
		logLevel = ERROR
	case "FATAL":
		logLevel = FATAL
	}
}

func SetIfaName(ifaName string) {
	ip = utils.GetDeviceIP(ifaName)
}

func SetExecutableName(name string) {

	index := strings.LastIndex(name, "/")
	if index == -1 {
		executableName = name
	} else {
		executableName = name[index+1:]
	}
	pid = os.Getpid()
}

func Log(level string, body string) {

	if len(body) > logBodyMaxLength {
		body = body[:logBodyMaxLength]
	}

	pc, file, line, ok := runtime.Caller(2)
	if !ok {
		fmt.Println("Unable to get caller information")
		return
	}

	fileName := filepath.Base(file)
	funcName := runtime.FuncForPC(pc).Name()
	funcNameParts := strings.Split(funcName, ".")
	lastFuncName := funcNameParts[len(funcNameParts)-1]

	timeFormat := "2006-01-02 15:04:05,000"
	currentTime := time.Now().Format(timeFormat)

	outBody := fmt.Sprintf("[%s]~[%s]~[%s]~[%d(0)]~[%s]~[%s@%s:%d]~[]: %s", currentTime, level, ip, pid, executableName, lastFuncName, fileName, line, body)
	if logEndpoint == "std::cout" {
		fmt.Printf("%s\n", outBody)
	} else if strings.HasPrefix(logEndpoint, "unix://") {
		sendToLogAgent(level, outBody)
	}
}

func sendToLogAgent(level string, body string) {
	tag := strings.ToLower(level)
	dlog.Write(logTag+"."+tag, body)
}

func Debug(format string, v ...any) {
	if logLevel <= DEBUG {
		Log("DEBUG", fmt.Sprintf(format, v...))
	}
}

func Info(format string, v ...any) {
	if logLevel <= INFO {
		Log("INFO", fmt.Sprintf(format, v...))
	}
}

func Warn(format string, v ...any) {
	if logLevel <= WARN {
		Log("WARN", fmt.Sprintf(format, v...))
	}
}

func Error(format string, v ...any) {
	if logLevel <= ERROR {
		Log("ERROR", fmt.Sprintf(format, v...))
	}
}

func Fatal(format string, v ...any) {
	if logLevel <= FATAL {
		Log("FATAL", fmt.Sprintf(format, v...))
	}
}

func Force(format string, v ...any) {
	Log("INFO", fmt.Sprintf(format, v...))
}
