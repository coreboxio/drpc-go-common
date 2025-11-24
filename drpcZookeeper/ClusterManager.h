#ifndef CLUSTERMANAGER_H
#define	CLUSTERMANAGER_H
#include <zookeeper/zookeeper.h>
#include <memory>
#include <string>
#include <vector>
#include <set>
#include <iostream>
#include <fstream>
#include <pthread.h>
#include <unistd.h>
#include <string.h>
#include <map>
#include <mutex>
#include <cstdio>
#include <cstdlib>
#include <cstdarg>
#include "ClusterManagerWrapper.h"

const char* const ROOT="/servers";
const char* const ROOT_SERVER_STATS="/server_stats";

typedef void (*LogCallbackFunc)(char*, char*);
void log(const std::string& level, const std::string& content);

inline char* formatString(const char* format, ...) {
    size_t bufferSize = 256;
    char* buffer = (char*)malloc(bufferSize);
    if (!buffer) return nullptr;

    va_list args;
    va_start(args, format);
    
    vsnprintf(buffer, bufferSize, format, args);
    
    va_end(args);
    return buffer;
}

inline std::vector<std::string>& split(const std::string& s, const std::string& delim, std::vector<std::string> &elems) {
	if(s.empty()) return elems;
	
	std::string::size_type pos_begin = s.find_first_not_of(delim);
	std::string::size_type comma_pos = 0;
	std::string tmp;
	while (pos_begin != std::string::npos){
		comma_pos = s.find_first_of(delim, pos_begin);
		if (comma_pos != std::string::npos){
			tmp = s.substr(pos_begin, comma_pos - pos_begin);
			pos_begin = comma_pos + 1;
		}
		else{
			tmp = s.substr(pos_begin);
			pos_begin = comma_pos;
		}
		if (!tmp.empty()){
			elems.push_back(tmp);
			tmp.clear();
		}
	}

	return elems;
}

class ServerStatus {
	int   _conns;
	int _threads;
	time_t _start_time;
	bool _online;
public:

	ServerStatus():_conns(0), _threads(0), _start_time(-1), _online(false){}
	ServerStatus(const std::string& str, time_t timestamp);
	ServerStatus(int conns, int threads, const std::string& reg = std::string()) 
		: _conns(conns), _threads(threads), _start_time(-1), _online(false){}
	int getConnections(){return _conns;}
	int getThreads(){return _threads;}
	time_t getStartTime(){return _start_time;}
	bool isOnline(){ return _online;};
	
	void setConnections(int conns){_conns = conns;}       
	void setThreads(int threads){ _threads = threads;}
	void setStartTime(time_t timestamp){ _start_time=timestamp;};
	void online(bool online){ _online = online;};
	
	std::string str();
};

class ClusterManager {
public: 
	ClusterManager(const std::string& zk_list, const std::string& credential, const std::string& project, const std::string& type, const std::vector<std::string>& interested_types, const std::string& addr,  int timeout = 15000);
	ClusterManager(const std::string& zk_list, const std::string& credential, const std::string& project, const std::vector<std::string>& interested_types,  int timeout = 15000);
	ClusterManager(const std::string& zk_list, const std::string& credential, const std::string& project, const std::string& type, const std::string& addr,  int timeout = 15000);
	static std::shared_ptr<ClusterManager> create(const std::string& selfEndpoint, const std::string myServerName, const std::string& zookeeperWatchList,
            const std::string& zookeeperEndpoints, const std::string& zookeeperCredential, const std::string& zookeeperProject);
    static void registerLogCallback(LogCallbackFunc callback);
	virtual ~ClusterManager();
	unsigned long getServerMapRevision(const std::string& type);
	unsigned long getStatusRevision(const std::string& type);
	std::map<std::string, unsigned long> getRevisionMap();
	int updateLocalServerMap(watcher_fn watcher=NULL);
	// get servers that we are interested in in the  ordered of access speed
	std::map< std::string, std::map<std::string, ServerStatus> > 
			getServerMap( const std::string& type = std::string(), const bool online_only=true);
	std::set<std::string> 
			getServerSet( const std::string& type = std::string(), const bool online_only=true);
	std::vector<std::string> 
			getServerVector( const std::string& type = std::string(), const bool online_only=true);
	void addWatchingType(const std::string& type) {_watching_types.push_back(type);};
	std::vector<std::string> watchingTypes() {return _watching_types;};
	void init(bool online_on_start = true);
	void online();
	void quitCluster();
	void setStatus(int conns, int threads);
	ServerStatus getStatus();
	std::string getRegion();
	std::string getStatusStr();
	time_t getStartTime(){ return _start_time;}
	
	friend void* thread_forwarder(void* context);
	friend void child_watcher(zhandle_t *zh, int type, int state, const char *path,void* ctx);
	friend void authCompletionCallback(int rc, const void* data);
protected:    
	virtual void updateStatus();
private:
	ClusterManager();
	ClusterManager(const ClusterManager& orig);
	void joinCluster(const std::string& addr, bool online=true);
	void reconnect_zookeeper();

	void checkParentPath();
	void recreateNode(const std::string& path, const std::string& value, int flag = ZOO_EPHEMERAL);
	void removeNode(const std::string& path);

	void init_zookeeper(const std::string& credential, int timeout);

	zhandle_t* _zh;
	std::string _credential;
protected:
	bool _authorized;
private:
	std::string _project;
	std::string _my_type;
	std::vector<std::string> _watching_types;
	std::string _my_addr;
	std::string _zookeeper_list;
	const std::string _status_path;
	const std::string _servers_path;

	int _timeout;
	// {type:rev}
	std::map<std::string, unsigned long>  _stat_revs;
	std::map<std::string, unsigned long> _revs;
	std::map< std::string, std::map<std::string, ServerStatus> > _server_map;
	std::mutex _mutex;
	pthread_t _renew_th;
	void* renew_thread();
	
	
	// for reporting its own status
	ServerStatus _my_status;
	pthread_mutex_t _status_mutex;
	time_t _start_time;
	
	bool _exit;
	
std::vector<std::string> _ordered_regions; 
};

inline void* thread_forwarder(void* context) {
	return static_cast<ClusterManager*>(context)->renew_thread();
}

inline void free_String_vector(struct String_vector *v) {
	if (v->data) {
		int32_t i;
		for (i=0; i<v->count; i++) {
			free(v->data[i]);
		}
		free(v->data);
		v->data = 0;
	}
}

inline void authCompletionCallback(int rc, const void* data){
	ClusterManager *cm = (ClusterManager *)data;
	cm->_authorized = (rc == ZOK) ? true : false;
	log("INFO", formatString("Zookeeper authrized : %d", cm->_authorized));
}
// this function is executed in zookeeper client thread    
inline void child_watcher(zhandle_t *zh, int type, int state, const char *path, void* ctx)
{
	ClusterManager* cm = (ClusterManager*)ctx;
	if(type==ZOO_SESSION_EVENT){
		log("INFO", formatString("Zookeeper state: %d", state));
	}
	else if(type==ZOO_CHILD_EVENT)
	{
		log("INFO", formatString("Zookeeper children have changed"));

		for(auto type : cm->_watching_types){
			std::string watched_path = "/" + cm->_project + std::string(ROOT) + "/"+ type;
			struct String_vector rtms;
			struct Stat pstat;
			int rc = zoo_wget_children2(cm->_zh, watched_path.c_str(), child_watcher, ctx, &rtms, &pstat);

			if(!rc)
			{
				std::lock_guard<std::mutex> lock(cm->_mutex);
				if(pstat.cversion == (int32_t)cm->_revs[type])
				{
					// servers have no change
					continue;
				}
				std::map<std::string, ServerStatus> address_map;
				// get status of each server
				for(int i=0; i < rtms.count; ++i)
				{
					std::string server (rtms.data[i]); 
					const int MAXBUFLEN = 2048;
					char buffer[MAXBUFLEN] = {0};
					int buflen = sizeof(buffer);
					std::string state_path = "/" + cm->_project + std::string(ROOT_SERVER_STATS)  + "/" + type + "/" + server;
					struct Stat stat;
					rc = zoo_get(cm->_zh, state_path.c_str(), 0, (char *)&buffer,  &buflen, &stat);
					if(rc){
						 log("ERROR", formatString("Zookeeper failed to get status of node %s, r=%d", state_path.c_str(), rc));
					}
					if(buflen < stat.dataLength){
						 log("ERROR", formatString("Zookeeper buffer too small, current buffer len: %d, node data len: %d", MAXBUFLEN, stat.dataLength));
					}
					ServerStatus status(buffer, stat.ctime/1000);
					address_map.insert(make_pair(server, status));
				}
				free_String_vector(&rtms);
				cm->_server_map[type]=std::move(address_map);
				cm->_revs[type] = pstat.cversion;           
				for(auto& server : cm->_server_map){
					for(auto& ip_stats : server.second){               
							log("INFO", formatString("Zookeeper watch change: server(%s), endpoint(%s), connections(%d), threads(%d), startTime(%ld), isOnline(%d)", server.first.c_str(), ip_stats.first.c_str(),
									ip_stats.second.getConnections(), 
									ip_stats.second.getThreads(),
									ip_stats.second.getStartTime(),
									ip_stats.second.isOnline()));
					}                
				} 
			} 
			else{
				log("ERROR", formatString("Zookeeper failed to get children node %s, r=%d",watched_path.c_str(), rc));
				return;
			}
		}

	   
	}

}

class ClusterManagerException : std::exception{
public:
	ClusterManagerException(const std::string& msg){_msg = msg;}
	~ClusterManagerException() throw(){}
	const char* what() const noexcept {return _msg.c_str();}

private:
	std::string _msg;
};

#endif	/* CLUSTERMANAGER_H */

