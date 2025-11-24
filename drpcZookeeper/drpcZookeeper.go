package drpcZookeeper

/*
#cgo CXXFLAGS: -std=c++11 -I. -DTHREADED
#cgo LDFLAGS: -L/usr/local/lib -lzookeeper_mt -L. -L./drpcZookeeper  -ldrpczookeeper -ldrpczookeeperwrapper -lstdc++
#include <stdlib.h>
#include "ClusterManagerWrapper.h"

void cppLogCallback(char* level, char* body);
*/
import "C"
import "unsafe"

var logCallback func(level string, content string) = nil 

//export cppLogCallback
func cppLogCallback(levelC *C.char, contentC *C.char) {
    level := C.GoString(levelC)
    content := C.GoString(contentC)

    if logCallback != nil {
        logCallback(level, content)
    }
}

func SetLogCallback(callback func(level string, content string)) {
    logCallback = callback
    C.registerLogCallback(C.LogCallbackFunc(C.cppLogCallback));
}

func InitClusterManager(selfEndpoint string, myServerName string, zookeeperWatchList string, 
    zookeeperEndpoints string, zookeeperCredential string, zookeeperProject string) {

	selfEndpointC := C.CString(selfEndpoint)
	defer C.free(unsafe.Pointer(selfEndpointC))
	
	myServerNameC := C.CString(myServerName)
	defer C.free(unsafe.Pointer(myServerNameC))
	
	zookeeperWatchListC := C.CString(zookeeperWatchList)
	defer C.free(unsafe.Pointer(zookeeperWatchListC))
	
	zookeeperEndpointsC := C.CString(zookeeperEndpoints)
	defer C.free(unsafe.Pointer(zookeeperEndpointsC))
	
	zookeeperCredentialC := C.CString(zookeeperCredential)
	defer C.free(unsafe.Pointer(zookeeperCredentialC))
	
	zookeeperProjectC := C.CString(zookeeperProject)
	defer C.free(unsafe.Pointer(zookeeperProjectC))

	C.initClusterManager(selfEndpointC, myServerNameC, zookeeperWatchListC, zookeeperEndpointsC, zookeeperCredentialC, zookeeperProjectC)

}

