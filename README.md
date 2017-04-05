# playback_cluster

## How to compile it:

gcc playback_cluster.c `pkg-config --cflags glib-2.0 gio-2.0` `pkg-config --libs glib-2.0 gio-2.0` `mysql_config --include` `mysql_config --libs` -o playback_cluster


## Usage:
  playback_cluster [OPTION...] multi-threaded MySQL loader

Help Options:
  -?, --help         Show help options

Application Options:
  -h, --host         List of hosts, coma separated
  -u, --user         Username with the necessary privileges
  -p, --password     User password
  -S, --socket       UNIX domain socket file to use for connection
  -o, --output       Where to send the input: MYSQL or SOCKET
  
  
## Example of how to run it:

### Spliter 

cat dd.log | ./playback_cluster -h 127.0.0.1:5000,127.0.0.1:5001

### Clients
nc -k -l 127.0.0.1 5001 | ./a.out -h 127.0.0.1:5000,127.0.0.1:5001 --output MYSQL -u root -p david -S /tmp/mysql.sock
