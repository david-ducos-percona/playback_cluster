#include <mysql.h>

char *username=NULL;
char *password=NULL;
char *socket_path=NULL;
char *db=NULL;
char *defaults_file=NULL;
guint port=5000;
guint mysql_port=3306;

struct thread_config {
   char *host;
   int port;
   GAsyncQueue * queue;
   GSocketConnection *connection;
   MYSQL *mysql_connection;
};

struct query {
   char *statement;
   float time;
};

