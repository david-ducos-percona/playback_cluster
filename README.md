# playback_cluster
Each main process SOCKET will split the amount of sockets configured, in a round robin fashion but taking in account the thread_id which is in the slow query log. Each MYSQL process will connect to one database an create a connection per thread_id. 
With this tool you will be able to split the traffic to several servers which are also connecting throw several connections. 

## How to compile it:
```
yum install glib2-devel
yum install mysql-devel
yum install openssl-devel

gcc playback_cluster.c `pkg-config --cflags glib-2.0 gio-2.0` `pkg-config --libs glib-2.0 gio-2.0` `mysql_config --include` `mysql_config --libs` -o playback_cluster
```

## Usage:
```
  playback_cluster [OPTION...] multi-threaded MySQL loader

Help Options:
  -?, --help         Show help options

Application Options:
  -h, --host         List of hosts, coma separated
  -u, --user         Username with the necessary privileges
  -p, --password     User password
  -S, --socket       UNIX domain socket file to use for connection
  -o, --output       Where to send the input: MYSQL or SOCKET
  ```


## Example of how to run it:

### Spliter 
```
tail -f --follow=descriptor general_log.log | pt-query-digest  --output slowlog --type genlog --no-report  | playback_cluster -o SOCKET -h 127.0.0.1:5000,127.0.0.1:5001
```
Since coreutils-8.24 the bahaviour of coreutil command tail changed, as it is actually following the descriptor that in older versions was not doing it.


### Clients
```
nc -k -l 127.0.0.1 5000 | playback_cluster -h 127.0.0.1:5000,127.0.0.1:5001 --output MYSQL -u root -p david -S /tmp/mysql.sock
```
```
nc -k -l 127.0.0.1 5001 | playback_cluster -h 127.0.0.1:5000,127.0.0.1:5001 --output MYSQL -u root -p david -S /tmp/mysql.sock
```

## TODO
I need to add the functionality to close the connection when it receives the admin command quit. 
