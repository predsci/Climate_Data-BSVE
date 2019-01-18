#ifndef mongodbFunctions_h__
#define  mongodbFunctions_h__

extern int gridfsControl (char **command, char **host, char **usr, char **pw, int *port, 
				char **database, char **collection,
                		char **targetdir, char **filename, char **file_id, char **error);

#endif // mongodbFunctions_h__
