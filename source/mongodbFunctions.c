#include <assert.h>
#include <mongoc.h>
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <libgen.h>


int
gridfsControl (char **command, char **host, char **user, char **pw, int *port,
		char **database, char **collection, 
		char **targetdir, char **filename, char **fileid, char **error)
{
   mongoc_gridfs_t *gridfs = NULL;
   mongoc_gridfs_file_t *file = NULL;
   mongoc_gridfs_file_list_t *list = NULL;
   mongoc_gridfs_file_opt_t opt = {0};
   mongoc_client_t *client = NULL;
   mongoc_stream_t *stream = NULL;
   bson_t filter;
   bson_t opts;
   bson_t child;
   bson_oid_t oid;
   bson_error_t bson_error;
   ssize_t r;
   char buf[4096];
   mongoc_iovec_t iov;
//   const char *command;
   int status = 0;
   char *filename1;
   char *file_id;

   mongoc_init ();
   file_id = *fileid;

   iov.iov_base = (void *) buf;
   iov.iov_len = sizeof buf;
 
   /* connect to localhost client */
   char *url = malloc(256);
   memset(url, 0, 256);
//		  "mongodb://bsve_at_06496:CfdHxw53@dataservices-mongodb.bsvecosystem.net:27017/?authSource=", 
   sprintf (url, "mongodb://%s:%s@%s:%d/?authSource=%s", 
                  *user, *pw, *host, *port, *database);
   client =
      mongoc_client_new (url);

//   mongoc_collection_t *col = mongoc_client_get_collection (client, *database, *collection);
//   print_all_documents(col); 

   if (client == NULL) {
     sprintf(*error, "%s %s%s", "ERROR: mongoc_client_new could not connect to ", url, "\n");
     free(url);
     status = 1;
     goto CLEANUP;
   } else { 
     free(url);
   }

   /* Send errors to error out  FIXME */
   if (! mongoc_client_set_error_api (client, 2)) {
     sprintf(*error, "ERROR: mongoc_client_set_error_api failure\n");
     status = 1;
     goto CLEANUP;
   }

   /* grab a gridfs handle in test prefixed by fs */
   gridfs = mongoc_client_get_gridfs (client, *database, *collection, &bson_error);
   if (gridfs == NULL) {
     sprintf(*error, "ERROR: mongoc_client_get_grids failure\n");
     status = 1;
     goto CLEANUP;
   }
   
   
   if ((strcmp (*command, "read") == 0) |
       (strcmp (*command, "readByFilename") == 0) | 
       (strcmp (*command, "readByID") == 0)) {

     if (strcmp (*command, "readByID") == 0) {
       bson_oid_init_from_string(&oid, file_id);
       bson_init(&filter);
       bson_append_oid(&filter, "_id", 3,  &oid);
       file = mongoc_gridfs_find_one_with_opts(gridfs, &filter, NULL, &bson_error);
       filename1 = (char *) mongoc_gridfs_file_get_filename (file);
     } else {
       file = mongoc_gridfs_find_one_by_filename(gridfs, *filename, &bson_error);
       filename1 = *filename;
     }

     if (file == NULL) {
       sprintf(*error, "%s", "ERROR: mongoc_gridfs_find_one_with_opts failure.\n");
       status = 1;
       goto CLEANUP;
     }


     stream = mongoc_stream_gridfs_new (file);
     if (stream == NULL) {
       sprintf(*error, "%s", "ERROR: mongoc_stream_gridfs_new returned NULL");
       status = 1;
       goto CLEANUP;
     }

     char *filepath = malloc (strlen(*targetdir) + strlen(filename1)+ 2);
     sprintf(filepath, "%s/%s", *targetdir, filename1);
     FILE *fid = fopen(filepath, "w+");
     if (fid == NULL) {
       sprintf(*error, "%s %d\n", "ERROR: in fopen of local file, errno = ", errno);
       status = 1;
       goto CLEANUP;
     }

     for (;;) {
         r = mongoc_stream_readv (stream, &iov, 1, -1, 0);
         if (r < 0) {
	    sprintf(*error, "%s %ld\n", "ERROR: mongoc_stream_readv returned error ", r);
            status = 1;
            goto CLEANUP;
         } 

         if (r == 0) {
            break;
         }

         if (fwrite (iov.iov_base, 1, r, fid) != r) {
            MONGOC_ERROR ("Failed to write to stdout. Exiting.\n");
            exit (1);
         }
     }

     fclose(fid);
     free(filepath);

     mongoc_stream_destroy (stream);
     mongoc_gridfs_file_destroy (file);

   } else if (strcmp (*command, "list") == 0) {
      bson_init (&filter);

      bson_init (&opts);
      bson_append_document_begin (&opts, "sort", -1, &child);
      BSON_APPEND_INT32 (&child, "filename", 1);
      bson_append_document_end (&opts, &child);

      list = mongoc_gridfs_find_with_opts (gridfs, &filter, &opts);

      bson_destroy (&filter);
      bson_destroy (&opts);

      while ((file = mongoc_gridfs_file_list_next (list))) {
         filename1 = (char *)mongoc_gridfs_file_get_filename (file);
         printf ("%s\n", filename1 ? filename1 : "?");

         mongoc_gridfs_file_destroy (file);
      }

      mongoc_gridfs_file_list_destroy (list);
   } else if (strcmp (*command, "write") == 0) {

      char *filepath = malloc (strlen(*targetdir) + strlen(*filename)+ 2);
      sprintf(filepath, "%s/%s", *targetdir, *filename);
      stream = mongoc_stream_file_new_for_path (filepath, O_RDONLY, 0);
      assert (stream);

      opt.filename = *filename;
 
      /* the driver generates a file_id for you */
      file = mongoc_gridfs_create_file_from_stream (gridfs, stream, &opt);
      assert (file);
      const bson_value_t* tmp = mongoc_gridfs_file_get_id (file);
      if (tmp->value_type != BSON_TYPE_OID) {
        sprintf(*error, "%s\n", "ERROR: mongoc_gridfs_file_get_id did not return a BSON_TYPE_OID");
	status = 1;
	goto CLEANUP;
      }

      bson_oid_t new_oid;
      
      bson_oid_copy ((const bson_oid_t*) &tmp->value.v_oid, &new_oid); 
      bson_oid_to_string(&new_oid, file_id);

      mongoc_gridfs_file_save (file);
      mongoc_gridfs_file_destroy (file);
   
   } else if ((strcmp (*command, "remove") == 0) | 
              (strcmp (*command, "removeByID") == 0) |
              (strcmp (*command, "removeByFilename") == 0)) {
     if (strcmp (*command, "removeByID") == 0) {
      
       bson_oid_init_from_string(&oid, file_id);
       bson_init(&filter);
       bson_append_oid(&filter, "_id", -1,  &oid);
       file = mongoc_gridfs_find_one_with_opts(gridfs, &filter, NULL, &bson_error);
       filename1 = (char *) mongoc_gridfs_file_get_filename (file);
     } else
       filename1 = *filename;

     if (!mongoc_gridfs_remove_by_filename(gridfs, filename1, &bson_error)) {
      sprintf(*error, "ERROR: mongoc_gridfs_remove_by_filename\n%s\n%s\n%s\n",
			bson_error.domain, bson_error.code, bson_error.message);
     } 
   } else {
      sprintf (*error, "%s", "Unknown command");
      status = 1;
   }

CLEANUP:
   if (gridfs != NULL) { mongoc_gridfs_destroy (gridfs); }
   if (client != NULL) { mongoc_client_destroy (client); }
   mongoc_cleanup ();
   return (status);
}

//                "mongodb://bsve_at_06496:CfdHxw53@dataservices-mongodb.bsvecosystem.net:27017/?authSource=",

void displayinfo (char *argv0) {
    fprintf (stderr, "Format: %s read  < file oid > < output dir> < collection >\n         %s write < full path to file> <collection> \n	 %s list  < collection >\n         %s remove < filename > < collecton >\n",
	  	argv0, argv0, argv0, argv0); 
    exit(1);
}

int main (int argc, char **argv) {
  char *command = argv[1];
  char *host = "dataservices-mongodb.bsvecosystem.net";
  char *user = "bsve_at_06496";
  char *pw = "CfdHxw53";
  int port = 27017;
  char *database = "dice_forecast_rdata";
  char *collection = "flu";
  char *targetdir = NULL;
  char *error = malloc(256);
  char *file_id = malloc(25);
  char *filename = NULL;
  int result;
  char *argv0 = basename(argv[0]);

  if ((argc < 2) | (argc > 5)) displayinfo(argv0);

  if ((strcmp (command, "read") == 0) | 
      (strcmp (command, "readByFilename") ==0) | 
      (strcmp (command, "readByID") == 0)) 
  {
    if (argc != 5) displayinfo(argv0);
    if (strcmp (command, "readByID") == 0) {
      sprintf(file_id, "%s", argv[2]);
      filename = NULL;
    }
    else
    {
      filename = argv[2];
      file_id = "";
    }
    targetdir = argv[3];
    collection = argv[4];
  } else {
    if (strcmp (command, "write") == 0) 
    {
      if (argc != 4) displayinfo(argv0); 
      filename = basename(argv[2]);      
      targetdir = dirname(argv[2]);
      collection = argv[3];
      file_id = malloc(25);
    } else {
      if (strcmp (command, "list") == 0) 
      {
        if (argc != 3) displayinfo(argv0);
	collection = argv[2];   
      } else {
        if ((strcmp (command, "remove") == 0) |
            (strcmp (command, "removeByFilename") == 0) |
            (strcmp (command, "removeByID") == 0))
        {      
          if (argc != 4) displayinfo(argv0);
          if (strcmp (command, "removeByID") == 0) {
            sprintf(file_id, "%s", argv[2]);
            filename = NULL;
          } else {
            file_id = NULL;
            filename = argv[2];
          }
          collection = argv[3];
        } else {
          displayinfo(argv0);
        }
      }
    }
  }

  result = gridfsControl (&command, &host, &user, &pw, &port, &database, &collection, &targetdir, &filename, &file_id, &error);
  
  if (strcmp (command, "write") == 0) { printf("%s\n", file_id); }
  free (error);
  if (strcmp(command, "read") != 0) { free (file_id); }
  return (result);
}

