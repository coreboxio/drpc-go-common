all:
	g++ -fpermissive -shared -o libdrpczookeeper.so -fPIC -DTHREADED ClusterManager.cpp -I/usr/local/include -std=c++11
	g++ -fpermissive -shared -o libdrpczookeeperwrapper.so -fPIC -DTHREADED ClusterManagerWrapper.cpp -I/usr/local/include -std=c++11

clean:
	rm -f libdrpczookeeper.so
	rm -f libdrpczookeeperwrapper.so

deploy:
	rsync -av libdrpczookeeper.so libdrpczookeeperwrapper.so /usr/local/lib/

