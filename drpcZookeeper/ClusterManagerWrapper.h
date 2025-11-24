#ifndef CM_WRAPPER_H
#define CM_WRAPPER_H
#include <string.h>

typedef void (*LogCallbackFunc)(char*, char*);

#ifdef __cplusplus
extern "C" {
#endif

    void registerLogCallback(LogCallbackFunc callback);
    void initClusterManager(const char* selfEndpoint, const char* myServerName, const char* zookeeperWatchList,
            const char* zookeeperEndpoints, const char* zookeeperCredential, const char* zookeeperProject);

#ifdef __cplusplus
}
#endif


#endif
