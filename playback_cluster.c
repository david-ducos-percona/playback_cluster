#include <stdio.h>
#include <string.h>
#include <glib.h>
#include <glib/gstdio.h>
#include <stdarg.h>
#include <stdlib.h>
#include <gio/gio.h>
#include "playback_cluster.h"
#include <mysql.h>

gchar *hostlist;
gchar *output;
gchar *input_type=NULL;

int host_length;
static GOptionEntry entries[] =
{
        { "host", 'h', 0, G_OPTION_ARG_STRING, &hostlist, "List of hosts, coma separated", NULL },
        { "user", 'u', 0, G_OPTION_ARG_STRING, &username, "Username with the necessary privileges", NULL },
        { "password", 'p', 0, G_OPTION_ARG_STRING, &password, "User password", NULL },
        { "socket", 'S', 0, G_OPTION_ARG_STRING, &socket_path, "UNIX domain socket file to use for connection", NULL },
        { "output", 'o', 0, G_OPTION_ARG_STRING, &output, "Where to send the input: MYSQL or SOCKET", NULL },
        { "type", 't', 0, G_OPTION_ARG_STRING, &input_type, "The input kind which can be GENERAL or SLOWLOG", NULL }
};

MYSQL * new_mysql_connection(char *host, int port, char *db);
void *process_mysql_queue(struct thread_config *config);
gboolean read_line(FILE *file, gboolean is_compressed, GString *data, gboolean *eof);

      // We considered that this regexp is enough to determine a new line
char * regex = "^[0-9]{4}-[0-9]{2}-[0-9]{2}.*\t";

void send_to_mysql_general(){
      GArray * free_indexes=g_array_new(FALSE,FALSE,sizeof(int));

      GData * queue_list;
      g_datalist_init(&queue_list);

      GData * process_list;
      g_datalist_init(&process_list);


      // We considered that this regexp is enough to determine a new line

      FILE *infile=stdin;
      gboolean eof=FALSE;
      GString *data=g_string_new("");
      read_line(infile,FALSE,data,&eof);

      GAsyncQueue* queue;
      GThread * thread;
      gboolean found;
      int i=0;
      while (!feof(infile)&& !eof){
         found=g_regex_match_simple(regex, data->str, 0, 0);
         if (found){
            gchar **elements = g_strsplit(data->str,"\t",4);
            gchar **elements2 = g_strsplit(elements[1]," ",2);
            char * thread_id =elements2[0];
            char * command =elements2[1];
            int thread_id_key = atoi(thread_id);
//            g_message("Thead_id: %s - %d",thread_id,thread_id_key);
            thread=NULL;
            queue=NULL;
            thread=g_datalist_id_get_data(&process_list,thread_id_key);

            if (thread==NULL){ // NULL will means that the Thread id was not found, which means that we need to establish a new connection
               if (g_strrstr(command,"Connect")) {
                 gchar ** elements3 = g_strsplit(elements[2]," ",4);
                 char *database=elements3[2];
                 g_message("Connecting %s to database: %s",thread_id,database);
                 // Thread id not found in the list of processes assigned | We need to create a new process
                 queue=g_async_queue_new();

                 // Initilizing Thread
                 struct thread_config *config=g_new(struct thread_config,1);
                 config->host=hostlist;
                 config->queue=queue;
                 config->port=mysql_port;
                 config->thread_id=thread_id;
                 config->mysql_connection = new_mysql_connection(config->host,config->port,database);
                 thread= g_thread_new("MySQL_Thread",(GThreadFunc)process_mysql_queue, config);

                 // Adding to the list of process
                 g_datalist_id_set_data(&process_list,thread_id_key,thread);
                 // Adding to the list of Threads id
                 g_datalist_set_data(&queue_list,thread_id,queue);
               }
            }else{
               if (g_strrstr(command,"Quit")) {
                 g_message("Removing Thead_id: %d",thread_id_key);
                 g_datalist_id_remove_data(&process_list,thread_id_key);
               }else{
                 // Thread id found in the list of processes assigned, so we need to send to the correct queue
                 queue=g_datalist_get_data(&queue_list,thread_id);
                 
               }
            }
            data=g_string_new("");
            GString *statement=g_string_new(elements[2]);
            read_line(infile,FALSE,data,&eof);
            float query_time=0;
            found=g_regex_match_simple (regex, data->str, 0, 0);

            // We are going to read the statement that needs to be send to the database
            while (!feof(infile)&& !eof && !found){
               if (data != NULL && data->str != NULL && !found)
                  statement=g_string_append(statement,data->str);
               data=g_string_new("");
               read_line(infile,FALSE,data,&eof);
               found=g_regex_match_simple (regex, data->str, 0, 0);
            }

            // At this point we are able to send the statement
            if (queue!=NULL && statement != NULL && statement->str != NULL && statement->len>2 && g_strrstr(command,"Execute") ){
               g_message("sending statement");
               struct query *q=g_new0(struct query,1);
               q->statement=statement->str;
               q->time=query_time;
               g_async_queue_push(queue, q);
            }
         }else{ 
            data=g_string_new("");
            read_line(infile,FALSE,data,&eof);
         }
      }

}

void send_to_mysql_slowlog(){ 
      GArray * free_indexes=g_array_new(FALSE,FALSE,sizeof(int));

      GData * queue_list;
      g_datalist_init(&queue_list);

      GData * process_list;
      g_datalist_init(&process_list);

      FILE *infile=stdin;
      gboolean eof=FALSE;
      GString *data=g_string_new("");
      read_line(infile,FALSE,data,&eof);

      GAsyncQueue* queue;

      while (!feof(infile)&& !eof){
         if (g_strrstr(data->str,"# Thread_id: ")){
            gchar **elements = g_strsplit(data->str,":",2);
            char * thread_id =elements[1];

            GThread * thread=g_datalist_get_data(&process_list,thread_id);

            if (thread==NULL){ // NULL will means that the Thread id was not found, which means that we need to establish a new connection
               // Thread id not found in the list of processes assigned | We need to create a new process
               queue=g_async_queue_new();

               // Initilizing Thread
               struct thread_config *config=g_new(struct thread_config,1);
               config->host=hostlist;
               config->queue=queue;
               config->port=mysql_port;
               config->mysql_connection = new_mysql_connection(config->host,config->port,NULL);
               thread= g_thread_new("MySQL_Thread",(GThreadFunc)process_mysql_queue, config);

               // Adding to the list of process
               g_datalist_set_data(&process_list,thread_id,thread);
               // Adding to the list of Threads id
               g_datalist_set_data(&queue_list,thread_id,queue);
            }else{
               // Thread id found in the list of processes assigned, so we need to send to the correct queue
               queue=g_datalist_get_data(&queue_list,thread_id);
            }
            data=g_string_new("");
            GString *statement=g_string_new("");
            read_line(infile,FALSE,data,&eof);
            float query_time=0;

            // We are going to read the statement that needs to be send to the database
            while (!feof(infile)&& !eof && !g_strrstr(data->str,"# Thread_id: ")){
              if (g_strrstr(data->str,"# Query_time: ")){
                  gchar **elements = g_strsplit(data->str," ",5);
                  query_time =atof(elements[3]);
              }
//            g_message("Sending line: %s",data->str);
//               g_async_queue_push(queue, data->str);
               if (data != NULL && data->str != NULL && data->str[0]!='#')
                  statement=g_string_append(statement,data->str);
               data=g_string_new("");
               read_line(infile,FALSE,data,&eof);
            }

            // At this point we are able to send the statement
            if (statement != NULL && statement->str != NULL && statement->len>2 ){
               struct query *q=g_new0(struct query,1);
               q->statement=statement->str;
               q->time=query_time;
               g_async_queue_push(queue, q);
            }
         }else{ // The line didn't start with "# Thread_id: ", at this point, if this happens, the line must be discarded
            data=g_string_new("");
            read_line(infile,FALSE,data,&eof);
         }
      }
}

GSocketConnection * new_connection(char *host, int port){
   GError * error = NULL;
   GSocketConnection * connection = NULL;
   GSocketClient * client = g_socket_client_new();
   connection = g_socket_client_connect_to_host (client, host, port, NULL, &error);
   if (error != NULL)
   {
      g_error("Trying to connect to %s on port %d: %s",host,port,error->message);
   }else{
      g_print ("Connection successful!\n");
   }
   return connection;
}

MYSQL * new_mysql_connection(char *host, int port,char *db){
   MYSQL *thrconn= mysql_init(NULL);

   my_bool reconnect = 1;
   mysql_options(thrconn, MYSQL_OPT_RECONNECT, &reconnect);

   if (!mysql_real_connect(thrconn, host, username, password, db, port, socket_path, CLIENT_MULTI_STATEMENTS)) {
      g_critical("Failed to connect to MySQL server: %s", mysql_error(thrconn));
      exit(EXIT_FAILURE);
   }
   mysql_options(thrconn, MYSQL_OPT_RECONNECT, &reconnect);
   return thrconn;
}

int send_mysql_data(MYSQL * connection,char *data,struct thread_config *config) {
  int error ;
//  g_message("Sending data");
  gint64 fromtime=g_get_real_time();
  error=mysql_real_query(connection, data, strlen(data));
  if (error) {
      g_critical("Error on %s: %s\n%s", config->thread_id,mysql_error(connection),data); 
      g_free(data);
  }else{
      mysql_store_result(connection);
  }
  return error;
}

int send_data(GSocketConnection * connection,char *data) {
  GError * error = NULL;
  GOutputStream * ostream = g_io_stream_get_output_stream (G_IO_STREAM (connection));
//  g_message("Sending data");
  gsize bytes_written;
//  if (strlen(data)>1000)
//    g_message("Line length: %d",strlen(data));

  g_output_stream_write_all  (ostream,
                          data, 
                          strlen(data), 
                          &bytes_written,
                          NULL,
                          &error);
  if (error != NULL)
  {
      g_error (error->message);
  }
  return 0;
}


gboolean read_line(FILE *file, gboolean is_compressed, GString *data, gboolean *eof) {
  const int buffersize=512;
  char buffer[buffersize];
  do {
    if (fgets(buffer, buffersize, file) == NULL) {
      if (feof(file)) {
        *eof= TRUE;
        buffer[0]= '\0';
      } else {
        return FALSE;
    }
  }
  if (buffer[0] != '\n' && strlen(buffer)>0)
    g_string_append(data, buffer);
  } while ( strlen(buffer)>1 && (buffer[strlen(buffer)-1] != '\n') && *eof == FALSE);
  return TRUE;
}

void *process_queue(struct thread_config *config) {
  for(;;) {
     char * line= (char*)g_async_queue_pop(config->queue);
     if (g_strrstr(line,"END THREAD"))
        break;
     send_data(config->connection,line);
  }
  return NULL;
}

void *process_mysql_queue(struct thread_config *config) {
  g_message("Adding new process");
  for(;;) {
    struct query *q =(struct query *)g_async_queue_pop(config->queue);
    char * line= q->statement;
    if (g_strrstr(line,"END THREAD"))
      break;
    g_message("One: %d | Other: %s",mysql_thread_id(config->mysql_connection),config->thread_id);
    send_mysql_data(config->mysql_connection,line,config);
  }
  return NULL;
}


// Actually just from stdin
// We can add, from file, from socket, others? do we need?
// element which is read is a slow query log
//

void *read_process_from_slowlog(GAsyncQueue**queue){
   FILE *infile=stdin;
   gboolean eof=FALSE;
   GString *data=g_string_new("");
   read_line(infile,FALSE,data,&eof);
   gint64 thread_id = 0;
   while (!feof(infile)&& !eof){
      if (g_strrstr(data->str,"# Thread_id: ")){
         gchar **elements = g_strsplit(data->str,":",2);
         thread_id = g_ascii_strtoll(elements[1],NULL,10);
         g_async_queue_push(queue[thread_id%host_length], data->str);
         data=g_string_new("");
         read_line(infile,FALSE,data,&eof);
         while (!feof(infile)&& !eof && data->str[0]=='#' && !g_strrstr(data->str,"# Thread_id: ")){
            g_async_queue_push(queue[thread_id%host_length], data->str);
            data=g_string_new("");
            read_line(infile,FALSE,data,&eof);
         }
         while (!feof(infile)&& !eof && data->str[0]!='#'){
            g_async_queue_push(queue[thread_id%host_length], data->str);
            data=g_string_new("");
            read_line(infile,FALSE,data,&eof);
         }
      }else{
         if (thread_id!=0) g_async_queue_push(queue[thread_id%host_length], data->str);
         data=g_string_new("");
         read_line(infile,FALSE,data,&eof);
      }
   }
   int i=0;
   for (; i < host_length;i++){
      g_async_queue_push(queue[i], "END THREAD");
   }
   return NULL;
}

void *read_process_from_general(GAsyncQueue**queue){
   FILE *infile=stdin;
   gboolean eof=FALSE;
   GString *data=g_string_new("");

   read_line(infile,FALSE,data,&eof);
   gint64 thread_id = 0;
   gboolean found;
   while (!feof(infile)&& !eof){
      found=g_regex_match_simple (regex, data->str, 0, 0);
      if (found){
         gchar **elements = g_strsplit(data->str,"\t",2);
         thread_id = g_ascii_strtoll(elements[1],NULL,10);
         g_async_queue_push(queue[thread_id%host_length], data->str);
         data=g_string_new("");
         read_line(infile,FALSE,data,&eof);
         found=g_regex_match_simple (regex, data->str, 0, 0);
         while (!feof(infile)&& !eof && !found){
            g_async_queue_push(queue[thread_id%host_length], data->str);
            data=g_string_new("");
            read_line(infile,FALSE,data,&eof);
            found=g_regex_match_simple (regex, data->str, 0, 0);
         }
      }else{
         if (thread_id!=0) g_async_queue_push(queue[thread_id%host_length], data->str);
         data=g_string_new("");
         read_line(infile,FALSE,data,&eof);
      }
   }
   int i=0;
   for (; i < host_length;i++){
      g_async_queue_push(queue[i], "END THREAD");
   }
   return NULL;
}


int main (int argc, char *argv[]){

GData * keylist;
g_datalist_init(&keylist);
char * data;
data = g_strdup("hola");
int id=1;
g_datalist_id_set_data(&keylist,id,data);
char * dd=g_datalist_id_get_data(&keylist,id);
g_message("%s",dd);
data = g_strdup("HOLA");
id = 2;
g_datalist_id_set_data(&keylist,id,data);
dd=g_datalist_id_get_data(&keylist,id);
g_message("%s",dd);

   GError *error= NULL;

   GOptionContext *context;
   context= g_option_context_new("multi-threaded MySQL loader");
   GOptionGroup *main_group= g_option_group_new("main", "Main Options", "Main Options", NULL, NULL);
   g_option_group_add_entries(main_group, entries);
   g_option_context_set_main_group(context, main_group);
   if (!g_option_context_parse(context, &argc, &argv, &error)) {
      g_print("option parsing failed: %s, try --help\n", error->message);
      exit(EXIT_FAILURE);
   }
   g_option_context_free(context);
   if (output==NULL) output="SOCKET";

   GSocketConnection * shostlist[200];

   g_message("%s",hostlist);

   gchar **host_list = g_strsplit(hostlist,",",-1);
   host_length=g_strv_length(host_list);

   if (g_strrstr(output,"SOCKET")){
      GAsyncQueue* queue[host_length];
      int i=0;
      GThread **threads= g_new(GThread*, host_length+1);
      for (; i < host_length;i++){
         gchar **host_port=g_strsplit(host_list[i],":",1);
         struct thread_config *config=g_new(struct thread_config,1);
         config->host=host_port[0];
         queue[i]=g_async_queue_new();
         config->queue=queue[i];
         if (g_strv_length(host_port)>1){
            config->port=g_ascii_strtoll(host_port[1],NULL,10);
         }else{
            config->port=port;
         }
         config->connection = new_connection(config->host,config->port);
         if (config->connection == NULL) exit(2);
         threads[i]= g_thread_new("SOCKET_Thread",(GThreadFunc)process_queue, config);
      }
      if (input_type!=NULL && g_strrstr(input_type,"SLOWLOG"))
        threads[host_length]=g_thread_new("Read_Thread",(GThreadFunc)read_process_from_slowlog,queue);
      else
      if (input_type!=NULL && g_strrstr(input_type,"GENERAL"))
        threads[host_length]=g_thread_new("Read_Thread",(GThreadFunc)read_process_from_general,queue);
      else{
        g_error("Input type not defined or incorrect");
        exit(1);
      }
        
      i=0;
      for (; i < host_length+1;i++){
         g_thread_join(threads[i]);
      }
   }else{  // MYSQL connection:
     g_message("MySQL");
     if (input_type!=NULL && g_strrstr(input_type,"SLOWLOG"))
        send_to_mysql_slowlog();
     else
      if (input_type!=NULL && g_strrstr(input_type,"GENERAL"))
         send_to_mysql_general();
      else{
        g_error("Input type not defined or incorrect");
        exit(1);
      }
   }
}




