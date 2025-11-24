#include "ClusterManager.h"
#include "ClusterManagerWrapper.h"

#ifdef __cplusplus
extern "C" {
#endif

static std::shared_ptr<ClusterManager> cmPtr = nullptr;

void registerLogCallback(LogCallbackFunc callback) {
    ClusterManager::registerLogCallback(callback);
}

void initClusterManager(const char* selfEndpoint, const char* myServerName, const char* zookeeperWatchList,
	const char* zookeeperEndpoints, const char* zookeeperCredential, const char* zookeeperProject) {
	cmPtr = ClusterManager::create(selfEndpoint, myServerName, zookeeperWatchList, zookeeperEndpoints, zookeeperCredential, zookeeperProject);
}

#ifdef __cplusplus
}
#endif
