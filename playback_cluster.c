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

int host_length;
static GOptionEntry entries[] =
{
        { "host", 'h', 0, G_OPTION_ARG_STRING, &hostlist, "List of hosts, coma separated", NULL },
        { "user", 'u', 0, G_OPTION_ARG_STRING, &username, "Username with the necessary privileges", NULL },
        { "password", 'p', 0, G_OPTION_ARG_STRING, &password, "User password", NULL },
        { "socket", 'S', 0, G_OPTION_ARG_STRING, &socket_path, "UNIX domain socket file to use for connection", NULL },
        { "output", 'o', 0, G_OPTION_ARG_STRING, &output, "Where to send the input: MYSQL or SOCKET", NULL }

};

GSocketConnection * new_connection(char *host, int port){

   GError * error = NULL;

   GSocketConnection * connection = NULL;
   GSocketClient * client = g_socket_client_new();

   connection = g_socket_client_connect_to_host (client,
                                                host,
                                                port,
                                                NULL,
                                                &error);

   if (error != NULL)
   {
//      g_error (error->message);
      g_error("Trying to connect to %s on port %d: %s",host,port,error->message);
   }else{
      g_print ("Connection successful!\n");
   }
   return connection;
}

MYSQL * new_mysql_connection(char *host, int port){
   MYSQL *thrconn= mysql_init(NULL);

//   mysql_options(thrconn, MYSQL_READ_DEFAULT_GROUP, "myloader");
   my_bool reconnect = 1;
   mysql_options(thrconn, MYSQL_OPT_RECONNECT, &reconnect);

   if (!mysql_real_connect(thrconn, host, username, password, NULL, port, socket_path, CLIENT_MULTI_STATEMENTS)) {
      g_critical("Failed to connect to MySQL server: %s", mysql_error(thrconn));
      exit(EXIT_FAILURE);
   }
   mysql_options(thrconn, MYSQL_OPT_RECONNECT, &reconnect);
   return thrconn;
}


int send_mysql_data(MYSQL * connection,char *data) {
  int error ;
//  g_message("Sending data");
  gint64 fromtime=g_get_real_time();
  error=mysql_real_query(connection, data, strlen(data));
  if (error) {
      g_critical("Error %s%s", mysql_error(connection),data); 
      g_free(data);
  }else{
//      g_message("OKKKK %s",data);
  }
  gint64 totime=g_get_real_time();
  MYSQL_RES * result;
  int state=0;
  while (!state){
     result=mysql_use_result(connection);
//     g_message("Fetching result %s",result);
     while (result !=NULL && mysql_fetch_row(result)!=NULL);
     mysql_free_result(result);
     state=mysql_next_result(connection);
  }
  return error;
}

int send_data(GSocketConnection * connection,char *data) {
  GError * error = NULL;
  GOutputStream * ostream = g_io_stream_get_output_stream (G_IO_STREAM (connection));
//  g_message("Sending data");
  g_output_stream_write  (ostream,
                          data, 
                          strlen(data), 
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
      send_mysql_data(config->mysql_connection,line);
   }
  return NULL;
}


// Actually just from stdin
// We can add, from file, from socket, others? do we need?
// element which is read is a slow query log
//

void *read_process(GAsyncQueue**queue){
   FILE *infile=stdin;
   gboolean eof=FALSE;
   GString *data=g_string_new("");
   read_line(infile,FALSE,data,&eof);
   gint64 thread_id = 0;
   while (!feof(infile)&& !eof){
      if (g_strrstr(data->str,"# Thread_id: ")){
         gchar **elements = g_strsplit(data->str,":",2);
         thread_id = g_ascii_strtoll(elements[1],NULL,10);
//         g_message("Thread_id: %d \t Possition of the server: %d",thread_id,thread_id%host_length);
         g_async_queue_push(queue[thread_id%host_length], data->str);
         data=g_string_new("");
         read_line(infile,FALSE,data,&eof);
         while (!feof(infile)&& !eof && data->str[0]=='#' && !g_strrstr(data->str,"# Thread_id: ")){
//            g_message("Discarding line: %s",data->str);
            g_async_queue_push(queue[thread_id%host_length], data->str);
            data=g_string_new("");
            read_line(infile,FALSE,data,&eof);
         } //while (!feof(infile)&& !eof && data->str[0]=='#' && !g_strrstr(data->str,"# Thread_id: "));
//         char *more_data=g_strdup_printf ("# Thread_id: %ld\n",thread_id);
//         send_data(shostlist[thread_id%host_length],more_data);
//         g_async_queue_push(queue[thread_id%host_length], more_data);
         while (!feof(infile)&& !eof && data->str[0]!='#'){
//            g_message("Thread_id %d : %s",thread_id,data->str);
            g_async_queue_push(queue[thread_id%host_length], data->str);
            data=g_string_new("");
            read_line(infile,FALSE,data,&eof);
         }

      }else{
   //      g_message("Discarding line: %s",data->str);
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
         threads[i]= g_thread_new("lala",(GThreadFunc)process_queue, config);
      }
      threads[host_length]=g_thread_new("lala",(GThreadFunc)read_process,queue);
      i=0;
      for (; i < host_length+1;i++){
         g_thread_join(threads[i]);
      }
   }else{
      GArray * free_indexes=g_array_new(FALSE,FALSE,sizeof(int));
      int max_id=0;
      GData * queue_list;
      GData * process_list;
      g_datalist_init(&process_list);
      g_datalist_init(&queue_list);
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
            
            if (thread==NULL){
               // Thread id not found in the list of processes assigned | We need to create a new process
               queue=g_async_queue_new();
               struct thread_config *config=g_new(struct thread_config,1);
               config->host=hostlist;
               config->queue=queue;
               config->port=mysql_port;
               config->mysql_connection = new_mysql_connection(config->host,config->port);
               thread= g_thread_new("lala",(GThreadFunc)process_mysql_queue, config);
               g_datalist_set_data(&process_list,thread_id,thread);
               g_datalist_set_data(&queue_list,thread_id,queue);
            }else{
               // Thread id found in the list of processes assigned, so we need to send to the correct queue
               queue=g_datalist_get_data(&queue_list,thread_id);
            }
            data=g_string_new("");
            GString *statement=g_string_new("");
            read_line(infile,FALSE,data,&eof);
            float query_time=0;
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
            if (statement != NULL && statement->str != NULL && statement->len>2 ){
               struct query *q=g_new0(struct query,1);
               q->statement=statement->str;
               q->time=query_time;
               g_async_queue_push(queue, q);
            }
         }else{
//         g_message("Discarding line: %s",data->str);
            
            data=g_string_new("");
            read_line(infile,FALSE,data,&eof);
         }
      }
   }
}




