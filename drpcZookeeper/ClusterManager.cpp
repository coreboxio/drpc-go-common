#include "ClusterManager.h"
#include <unistd.h>
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <random>
#include <algorithm>
#include <memory>

ServerStatus::ServerStatus(const std::string& str, time_t timestamp){
    _conns = _threads = 0;
    _start_time = timestamp;
    std::vector<std::string> strs;
	split(str, ";", strs);
    for(std::string& kv : strs){
        std::vector<std::string> vec;
		split(kv, ":", vec);
        if(!vec[0].compare("conns")){
            _conns = atoi(vec[1].c_str());
        }
        else if(!vec[0].compare("thrds")){
            _threads = atoi(vec[1].c_str());
        }
        else if(!vec[0].compare("online")){
            _online = (atoi(vec[1].c_str()))?true : false;
        }
    }
}

std::string ServerStatus::str(){
    std::stringstream sstr;
    sstr <<  "conns:" << getConnections()
            <<";thrds:"<< getThreads()
            <<";online:"<< isOnline();
    return sstr.str();
}

static LogCallbackFunc g_LogCallback = nullptr;

void log(const std::string& level, const std::string& content) {
	if (g_LogCallback) {
		g_LogCallback((char*)level.c_str(), (char*)content.c_str());
    }
}

void ClusterManager::registerLogCallback(LogCallbackFunc callback) {
	g_LogCallback = callback;
}

std::shared_ptr<ClusterManager> ClusterManager::create(const std::string& selfEndpoint, const std::string myServerName, const std::string& zookeeperWatchList, 
		const std::string& zookeeperEndpoints, const std::string& zookeeperCredential, const std::string& zookeeperProject) {
    std::vector<std::string> interestedVec;
    split(zookeeperWatchList, ", \t", interestedVec);
    interestedVec.push_back(myServerName);

    std::shared_ptr<ClusterManager> cmPtr(
        new ClusterManager(
            zookeeperEndpoints,
			zookeeperCredential,
			zookeeperProject,
            myServerName,
            interestedVec,
            selfEndpoint 
        )
    );

    cmPtr->init();

    return cmPtr;
}

void ClusterManager::init_zookeeper(const std::string& credential, int timeout){
    pthread_mutex_init(&_status_mutex, NULL);
    zoo_deterministic_conn_order(1);
    zoo_set_debug_level((ZooLogLevel)0);
    _zh = zookeeper_init(_zookeeper_list.c_str(), 0, timeout, 0, 0, 0);
    _credential = credential;
    for (int i = 0; i < 5; i++) {
        int rc = zoo_add_auth(_zh, "digest", credential.c_str(), credential.length(), authCompletionCallback, this);
        if(rc != ZOK){
            if (i == 4) {
                log("ERROR", formatString("%s, credential=%s", zerror(rc), _credential.c_str()));
                throw new ClusterManagerException("failed to send auth!");
            } else {
                log("ERROR", formatString("%s, credential=%s, retry after 1s", zerror(rc), _credential.c_str()));
                sleep(1);
                continue;
            }
        } else
            break;
    }
    _start_time = time(NULL);
}

ClusterManager::ClusterManager(const std::string& zk_list, const std::string& credential, const std::string& project, const std::string& type, const std::string& addr, int timeout)
: _project(project), _my_type(type), _my_addr(addr), _zookeeper_list(zk_list),
        _timeout(timeout), _my_status(0,0), _exit(false)
{
	if(project.length() == 0 || credential.length() == 0){
		throw new ClusterManagerException("project should be set!");
	}
	init_zookeeper(credential, timeout);
}

ClusterManager::ClusterManager(const std::string& zk_list, const std::string& credential, const std::string& project, const std::string& type, const std::vector<std::string>& interested_types, const std::string& addr, int timeout)
: _project(project),  _my_type(type), _watching_types(interested_types), _my_addr(addr), _zookeeper_list(zk_list),
        _timeout(timeout), _my_status(0,0), _exit(false)
{
	if(project.length() == 0 || credential.length() == 0){
		throw new ClusterManagerException("project & credential should be set!");
	}
    for(auto& type : _watching_types){
        _revs.insert(make_pair(type, 0));
        _stat_revs.insert(make_pair(type, 0));
    }
    init_zookeeper(credential, timeout);
}

ClusterManager::ClusterManager(const std::string& zk_list, const std::string& credential, const std::string& project, const std::vector<std::string>& interested_types,  int timeout)
: _project(project), _watching_types(interested_types),  _zookeeper_list(zk_list),
        _timeout(timeout),  _my_status(0,0), _exit(false)
{
	if(project.length() == 0 ||  credential.length() == 0){
		throw new ClusterManagerException("project should be set!");
	}
    for(auto& type : _watching_types){
        _revs.insert(make_pair(type, 0));
        _stat_revs.insert(make_pair(type, 0));
    }
    init_zookeeper(credential, timeout);
}

void ClusterManager::init(bool online_on_start){
    // should retry if exception occurs
    while(true){
        try{
            if(_my_type.size())
                joinCluster(_my_addr, online_on_start);
            break;
        }
        catch(ClusterManagerException& ex){
            log("ERROR", formatString("%s", ex.what()));
            sleep(5);
        }
    }    

    // add watcher and get initial value for map
    getServerMap();
    int rc = pthread_create(&_renew_th, NULL, thread_forwarder, (void *)this);
    if(rc)
    {
        throw ClusterManagerException("failed to create renew thread");
    }  

}

void ClusterManager::quitCluster(){
    zookeeper_close(_zh);
    std::string path = "/" + _project + std::string(ROOT_SERVER_STATS) + "/" + _my_type + "/" + _my_addr;
    removeNode(path);
    path = "/" + _project + std::string(ROOT) + "/" + _my_type + "/" + _my_addr;
    removeNode(path);       
}

void ClusterManager::setStatus(int conns, int threads){
    pthread_mutex_lock(&_status_mutex);
    _my_status.setConnections(conns); 
    _my_status.setThreads(threads); 
    pthread_mutex_unlock(&_status_mutex);    
}

ServerStatus ClusterManager::getStatus(){
    pthread_mutex_lock(&_status_mutex);
    ServerStatus status = _my_status; 
    pthread_mutex_unlock(&_status_mutex);
    return status;
}

std::string ClusterManager::getStatusStr(){
    pthread_mutex_lock(&_status_mutex);
    ServerStatus status = _my_status; 
    pthread_mutex_unlock(&_status_mutex);
    return status.str();
}


ClusterManager::~ClusterManager() {
    // what if zookeeper client is locking the mutex??
    zookeeper_close(_zh);
    _exit = true;
    if(_renew_th)
    	pthread_join(_renew_th, NULL);
    pthread_mutex_destroy(&_status_mutex);
}

void ClusterManager::checkParentPath(){
    std::vector<std::string> roots = {"/" + _project, "/" + _project + ROOT, "/" + _project + ROOT_SERVER_STATS};
    for(size_t i = 0; i < roots.size(); ++i)
    {
    	std::string path = roots[i];
        int rc = zoo_exists(_zh, path.c_str(), 0, NULL);
        if(rc == ZNONODE)
        {        
            int rc1 = zoo_create(_zh, path.c_str(), "",
                    0, &ZOO_CREATOR_ALL_ACL, 0, 0, 0);
            if(rc1 != ZOK){
                std::stringstream msgstr;
                msgstr << "failed to create node "<< path <<", " << zerror(rc);
                throw ClusterManagerException(msgstr.str());            
            }               
        }
        if(i == 0)
        	continue;

        if(_my_type.length() > 0){
			path += "/" + _my_type;
			rc = zoo_exists(_zh, path.c_str(), 0, NULL);
			if(rc == ZNONODE)
			{
				int rc1 = zoo_create(_zh, path.c_str(), "",
						0, &ZOO_CREATOR_ALL_ACL, 0, 0, 0);
				if(rc1 != ZOK){
					std::stringstream msgstr;
					msgstr << "failed to create node "<< path <<", code=" << rc;
					throw ClusterManagerException(msgstr.str());
				}
			}
        }
    }
}

void ClusterManager::removeNode(const std::string& path){
    int rc = zoo_delete(_zh, path.c_str(), -1);
    if(rc == ZOK){
		log("INFO", formatString("Zookeeper remove node %s", path.c_str()));
    }
    else if(rc == ZNONODE){
	   log("INFO", formatString("Zookeeper node %s already removed", path.c_str()));
    }
    else{
        std::stringstream msgstr;
        msgstr << "removeNode: failed to delete node "<< path <<", " << zerror(rc);
        throw ClusterManagerException(msgstr.str());            
    }      
}
void ClusterManager::recreateNode(const std::string& path, const std::string& value, int flag)
{
    int rc = zoo_exists(_zh, path.c_str(), 0, NULL);
    if(rc == ZOK){
        //remove already existing node
        int rc1 = zoo_delete(_zh, path.c_str(), -1);
        if(rc1 == ZOK){
            log("INFO", formatString("Zookeeper remove node %s", path.c_str()));
        }
        else if(rc1 == ZNONODE){
            log("INFO", formatString("Zookeeper node %s has already been removed", path.c_str())); 
        }        
        else{
            std::stringstream msgstr;
            msgstr << "recreateNode: failed to delete node "<< path <<", " << zerror(rc);
            throw ClusterManagerException(msgstr.str());            
        }      
    }  
    // create a node
    rc = zoo_create(_zh, path.c_str(), value.c_str(),
        value.size(), &ZOO_CREATOR_ALL_ACL, flag, 0, 0);
    if(rc)
    {
        std::stringstream msgstr;
        msgstr << "recreateNode: failed to create node "<< path <<", " << zerror(rc);
        throw ClusterManagerException(msgstr.str());
    }    
}


void ClusterManager::online(){
    quitCluster();
    joinCluster(_my_addr, true);
}

void ClusterManager::joinCluster(const std::string& addr, bool online) {
    checkParentPath();
    
    // create initial serer status nodes
    // status are stored as "load:5.0;conns:123;..."
    std::string path = "/" + _project + std::string(ROOT_SERVER_STATS) + "/" + _my_type + "/" + addr;
    log("INFO", formatString("Zookeeper recreate node %s", path.c_str()));
    recreateNode(path, "");
    _my_status.online(online);
    updateStatus();    
    
    path = "/" + _project + std::string(ROOT) + "/" + _my_type + "/" + addr;
    log("INFO", formatString("Zookeeper recreate node %s", path.c_str()));
    recreateNode(path, addr);   
}

int ClusterManager::updateLocalServerMap(watcher_fn watcher){
        for(auto type : _watching_types){
            std::string watched_path = "/" + _project + std::string(ROOT) + "/" + type;
            struct String_vector rtms;
            struct Stat pstat;
            int rc = zoo_wget_children2(_zh, watched_path.c_str(), watcher, this, &rtms, &pstat);
            if(!rc)
            {
                // servers have no change
                std::lock_guard<std::mutex> lock(_mutex);
                /*
                if(pstat.cversion == (int32_t)_revs[type])
                {
                    continue;
                }*/
                std::map<std::string, ServerStatus> address_map;
                // get status of each child
                for(int i=0; i < rtms.count; ++i)
                {
                    std::string server (rtms.data[i]); 
                    const int MAXBUFLEN = 2048;
                    char buffer[MAXBUFLEN] = {0};
                    int buflen = sizeof(buffer);
                    std::string state_path = "/" + _project + std::string(ROOT_SERVER_STATS)  + "/" + type + "/" + server;
                    struct Stat stat;
                    rc = zoo_get(_zh, state_path.c_str(), 0, (char *)&buffer,  &buflen, &stat);
                    if(rc){
                         log("ERROR", formatString("Zookeeper failed to get status of node %s, (%s)", state_path.c_str(), zerror(rc)));
                    }
                    if(buflen < stat.dataLength){
                         log("ERROR", formatString("Zookeeper buffer too small, current buffer len: %s, node data len: %d", MAXBUFLEN, stat.dataLength));
                    }
                    ServerStatus status(buffer, stat.ctime/1000);
                    address_map.insert(make_pair(server, status));
                }
                free_String_vector(&rtms);
                _server_map[type]=std::move(address_map);
                _revs[type] = pstat.cversion;
            } 
            else{
                log("ERROR", formatString("Zookeeper failed to get children node %s, (%s)", watched_path.c_str() , zerror(rc)));
                return rc;
            } 
            // update stat rev
            char buffer[1] = {0};
            int buflen = sizeof(buffer); 
            std::string state_path = "/" + _project + std::string(ROOT_SERVER_STATS)  + "/" + type;
            struct Stat stat;
            rc = zoo_get(_zh, state_path.c_str(), 0, (char *)&buffer,  &buflen, &stat);
            if(rc){
                 log("ERROR", formatString("Zookeeper failed to get status of node %s, (%s)", state_path.c_str(), zerror(rc)));
            }
            _stat_revs[type] = stat.version;
        }
        std::lock_guard<std::mutex> lock (_mutex);
        for(auto& server : _server_map){
            for(auto& ip_stats : server.second){               
                log("INFO", formatString("Zookeeper renew: %s\t%s\t%d\t%d\t%ld\t%d",server.first.c_str(), ip_stats.first.c_str(),
                        ip_stats.second.getConnections(), 
                        ip_stats.second.getThreads(),
                        ip_stats.second.getStartTime(),
                        ip_stats.second.isOnline()));
            }                
        }
        return 0;
}

unsigned long ClusterManager::getServerMapRevision(const std::string& type){
    return _revs.at(type);
}

unsigned long ClusterManager::getStatusRevision(const std::string& type){
    return _stat_revs.at(type);       
}

std::map<std::string, unsigned long> ClusterManager::getRevisionMap()
{
    return _revs;
}

std::map< std::string, std::map<std::string, ServerStatus> > 
ClusterManager::getServerMap(const std::string& type, bool online_only){

    if(!_server_map.size()){
        updateLocalServerMap(child_watcher);          
    }
    if(!type.size()){   
        return _server_map;        
    }
    else{
        //filter type
        std::map< std::string, std::map<std::string, ServerStatus> > ret;
        _mutex.lock();
        auto server_map = _server_map;
        _mutex.unlock(); 
        if(server_map.find(type)  != server_map.end() ){
            for( auto &kv : server_map[type]){                
                if(online_only && !kv.second.isOnline()){
                    continue;
                }
                ret[type].insert(std::move(kv));
            }
        }
        return ret;
    }
}

std::set<std::string> 
ClusterManager::getServerSet(const std::string& type, const bool online_only){
	std::set<std::string> ret;
    if(!_server_map.size()){
        updateLocalServerMap(child_watcher);          
    }
    if(type.length()>0){   
        //filter type
        _mutex.lock();
        if(_server_map.find(type)  != _server_map.end() ){
            for( auto &kv : _server_map[type]){
                if(online_only && !kv.second.isOnline()){
                    continue;
                }
                ret.insert(kv.first);
            }
        }
        _mutex.unlock(); 
    }
	return ret;
}

std::vector<std::string> 
ClusterManager::getServerVector(const std::string& type, const bool online_only){
	std::vector<std::string> ret;
    if(!_server_map.size()){
        updateLocalServerMap(child_watcher);
    }
    if(type.length()>0){   
        //filter type
        _mutex.lock();
        if(_server_map.find(type)  != _server_map.end() ){
            for( auto &kv : _server_map[type]){
                if(online_only && !kv.second.isOnline()){
                    continue;
                }
                ret.push_back(kv.first);
            }
        }
        _mutex.unlock(); 
    }
	return ret;
}


// update my status to zookeeper
void ClusterManager::updateStatus()
{
    std::string server_status = getStatusStr();
    std::string path = "/" + _project + std::string(ROOT_SERVER_STATS)  + "/" + _my_type + "/" + _my_addr ;
    int rc = zoo_set(_zh, path.c_str(), server_status.c_str(), server_status.size(), -1);

    if(rc == ZINVALIDSTATE)
    {
        log("ERROR", formatString("Zookeeper invalid zhandle state, retry now"));
        reconnect_zookeeper();
        rc = zoo_set(_zh, path.c_str(), server_status.c_str(), server_status.size(), -1);
    }

    if(rc)
    {
        log("ERROR", formatString("Zookeeper failed to update status of %s, (%s)", path.c_str(), zerror(rc)));
        return;
    }
    // set parent node value to increment its data version number, we use this 
    // version numer as the version of status change. client now can tell  the 
    // change of status node through  this
    path = "/" + _project + std::string(ROOT_SERVER_STATS)  + "/" + _my_type ;
    rc = zoo_set(_zh, path.c_str(), "", 0, -1);
    if(rc)
    {
        log("ERROR", formatString("Zookeeper failed to update parental node of status"));
    }    
}

void ClusterManager::reconnect_zookeeper()
{
    zookeeper_close(_zh);
    _zh = zookeeper_init(_zookeeper_list.c_str(), 0, _timeout, 0, 0, 0); 
    int rc = zoo_add_auth(_zh, "digest", _credential.c_str(), _credential.length(), authCompletionCallback, this);
    if(rc != ZOK){
        log("INFO", formatString("%s, credential=%s", zerror(rc), _credential.c_str()));
        throw new ClusterManagerException("failed to send auth!");
    }
    try{
        if(_my_type.size()){
            joinCluster(_my_addr);
        }
    }
    catch(ClusterManagerException& ex){
        log("ERROR", formatString("%s", ex.what()));
    }
}

void* ClusterManager::renew_thread()
{
    while(!_exit)
    {
        if(_my_type.size())
            updateStatus();
    
        if(!this->_watching_types.size()){

            int cyc = 100 * 30;
            while (!_exit && cyc--)
                usleep(10000);

            if(_exit)
            {
                pthread_exit(NULL);
            }
        }
        int rc = updateLocalServerMap();
        
        if(rc)
        {
            log("ERROR", formatString("Zookeeper renewing servers error :%s", zerror(rc)));
            // handling session expiration, we need to reinit our handle and rejoin cluster
            if(rc == ZINVALIDSTATE )
            {
                reconnect_zookeeper();
            }
        }

        int cyc = 100 * 30;
        while (!_exit && cyc--)
            usleep(10000);

        if(_exit)
        {
            pthread_exit(NULL);
        }
    }
    return NULL;
}

// g++ -fpermissive -shared -o libdrpczookeeper.so -fPIC -DTHREADED ClusterManager.cpp -I/usr/local/include -std=c++11

