#include <assert.h>
#include <mongoc.h>
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <libgen.h>
#include "mongodbFunctions.h"

int main (int argc, char **argv) {
  char *command = argv[1];

  char *host = "dataservices-mongodb.bsvecosystem.net";
  char *user = "bsve_at_06496";
  char *pw = "CfdHxw53";
  int port = 27017;
  char *database = "dice_forecast_rdata";
  char *collection = NULL;
  char *targetdir = NULL;
  char *error = malloc(256);
  char *file_id = NULL;
  char *filename = NULL;
  int result;

  memset(error,0,256);

  if (command == NULL) {
    fprintf (stderr, "Format:\n %s readByFilename <filename> <outputDir> <collection>\n %s readByID < file oid > <outputDir>  <collection>\n %s write < path to  directory > <filename> <collection> \n %s list <collection> \n %s removeByID < file oid > <collection>\n %s removeByFilename <filename> <collectin> \n",
            argv[0], argv[0], argv[0], argv[0], argv[0], argv[0]);
    exit(1);
  }

  if ((strcmp (command, "read") == 0) |
      (strcmp (command, "readByFilename") == 0) |
      (strcmp (command, "readByID") == 0))
  {
    if (strcmp (command, "readByID") == 0) {
      file_id = argv[2];
      filename = NULL;
    }
    else {
      filename = argv[2];
      file_id = NULL;
    }
    targetdir = argv[3];
    collection = argv[4];
  } else {
    if (strcmp (command, "write") == 0)
    {
      filename = argv[3];
      targetdir = argv[2];
      collection = argv[4];
      file_id = malloc(25);
    } else {
      if (strcmp(command, "list") == 0)
      {
        file_id = malloc(25);
        memset(file_id, 0, 25);
        collection = argv[2];
        filename = NULL;
        targetdir = NULL;
      } else {
        if ((strcmp(command, "remove") == 0) |
            (strcmp(command, "removeByID") == 0) |
            (strcmp(command, "removeByFilename") == 0)) {
          if (strcmp(command, "removeByID") == 0) {
            file_id = argv[2];
            filename = NULL;
          } else {
            filename = argv[2];
            file_id = NULL;
          }
          collection = argv[3];
        } else {
          fprintf (stderr, "Format:\n %s readByFilename <filename> <outputDir> <collection>\n %s readByID < file oid > <outputDir>  <collection>\n %s write < path to  directory > <filename> <collection> \n %s list <collection> \n %s removeByID < file oid > <collection>\n %s removeByFilename <filename> <collectin> \n",
                  argv[0], argv[0], argv[0], argv[0], argv[0], argv[0]);
          exit(1);
        }
      }
    }
  }

  result = gridfsControl (&command, &host, &user, &pw, &port, &database, &collection, &targetdir, &filename, &file_id, &error);

  fprintf (stderr, "%s\n", error);
  if (file_id != NULL)  fprintf (stdout, "%s\n", file_id);
  if (filename != NULL)  fprintf (stdout, "%s\n", filename);
  

  free (error);
  if (strcmp (command, "write") == 0) { free (file_id); }
  fprintf(stdout, "\n");
  return (result);
}

