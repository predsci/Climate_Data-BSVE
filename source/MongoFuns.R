
# Set of functions for reading/writing to BSVE mongodb

# Write local copy 'targetdir/Pop_map_NOAA2GADM.Rds' to MongoDB 
WriteNOAA_map <- function(targetdir="./") {
  command <- "write"
  host <- "dataservices-mongodb.bsvecosystem.net"
  user <- "bsve_at_06496"
  pw <- "CfdHxw53"
  port <- 27017
  database <- "dice_forecast_rdata"
  collection <- "dice_data_aux"
  filename <- "Pop_map_NOAA2GADM.Rds"
  # targetdir = "/Users/turtle/Dropbox/LEPR03/data/climate/"
  # targetdir = ""
  file_id <- ""
  error <- str_pad(" ", 250)	
  
  rtn1 <-.C("gridfsControl", 
            command = as.character(command), 
            host = as.character(host), 
            user = as.character(user), 
            pw = as.character(pw), 
            port = as.integer(port), 
            database = as.character(database), 
            collection = as.character(collection), 
            # targetdir = "savedFiles",
            targetdir = as.character(targetdir),
            filename = filename , 
            file_id = "", 
            error = as.character(error)
  )
  return(rtn1)
}

# Read MongoDB copy of 'Pop_map_NOAA2GADM.Rds' to local directory targetdir
ReadNOAA_map <- function(targetdir="./") {
  command <- "readByFilename"
  host <- "dataservices-mongodb.bsvecosystem.net"
  user <- "bsve_at_06496"
  pw <- "CfdHxw53"
  port <- 27017
  database <- "dice_forecast_rdata"
  collection <- "dice_data_aux"
  filename <- "Pop_map_NOAA2GADM.Rds"
  # targetdir = "/Users/turtle/Dropbox/LEPR03/data/climate/"
  # targetdir = "./"
  file_id <- ""
  error <- str_pad(" ", 250)	
  
  rtn1 <-.C("gridfsControl", 
            command = as.character(command), 
            host = as.character(host), 
            user = as.character(user), 
            pw = as.character(pw), 
            port = as.integer(port), 
            database = as.character(database), 
            collection = as.character(collection), 
            # targetdir = "savedFiles",
            targetdir = as.character(targetdir),
            filename = filename , 
            file_id = "", 
            error = as.character(error)
  )
  return(rtn1)
}

# Delete MongoDB copy of 'Pop_map_NOAA2GADM.Rds' 
RemoveNOAA_map <- function() {
  command <- "removeByFilename"
  host <- "dataservices-mongodb.bsvecosystem.net"
  user <- "bsve_at_06496"
  pw <- "CfdHxw53"
  port <- 27017
  database <- "dice_forecast_rdata"
  collection <- "dice_data_aux"
  filename <- "Pop_map_NOAA2GADM.Rds"
  # targetdir = "/Users/turtle/Dropbox/LEPR03/data/climate/"
  targetdir = "./"
  file_id <- ""
  error <- str_pad(" ", 250)	
  
  rtn1 <-.C("gridfsControl", 
            command = as.character(command), 
            host = as.character(host), 
            user = as.character(user), 
            pw = as.character(pw), 
            port = as.integer(port), 
            database = as.character(database), 
            collection = as.character(collection), 
            # targetdir = "savedFiles",
            targetdir = as.character(targetdir),
            filename = filename , 
            file_id = "", 
            error = as.character(error)
  )
  return(rtn1)
}

# Write local file 'targetdir/filename' to MondoDB in the specified collection with the same filename.
WriteToMongo <- function(collection=NULL, filename=NULL, targetdir=NULL) {
  command <- "write"
  host <- "dataservices-mongodb.bsvecosystem.net"
  user <- "bsve_at_06496"
  pw <- "CfdHxw53"
  port <- 27017
  database <- "dice_forecast_rdata"
  file_id <- ""
  error <- str_pad(" ", 250)	
  
  rtn1 <-.C("gridfsControl", 
            command = as.character(command), 
            host = as.character(host), 
            user = as.character(user), 
            pw = as.character(pw), 
            port = as.integer(port), 
            database = as.character(database), 
            collection = as.character(collection), 
            targetdir = as.character(targetdir),
            filename = as.character(filename), 
            file_id = "", 
            error = as.character(error)
  )
  return(rtn1)
}

# Read 'filename' from MondoDB collection and write to 'targetdir/filename'.
ReadFromMongo <- function(collection=NULL, filename=NULL, targetdir=NULL) {
  command <- "read"
  host <- "dataservices-mongodb.bsvecosystem.net"
  user <- "bsve_at_06496"
  pw <- "CfdHxw53"
  port <- 27017
  database <- "dice_forecast_rdata"
  file_id <- ""
  error <- str_pad(" ", 250)	
  
  rtn1 <-.C("gridfsControl", 
            command = as.character(command), 
            host = as.character(host), 
            user = as.character(user), 
            pw = as.character(pw), 
            port = as.integer(port), 
            database = as.character(database), 
            collection = as.character(collection), 
            targetdir = as.character(targetdir),
            filename = as.character(filename), 
            file_id = "", 
            error = as.character(error)
  )
  return(rtn1)
}

# Delete 'filename' from MondoDB collection
RemoveFromMongo <- function(collection=NULL, filename=NULL, targetdir=NULL) {
  command <- "remove"
  host <- "dataservices-mongodb.bsvecosystem.net"
  user <- "bsve_at_06496"
  pw <- "CfdHxw53"
  port <- 27017
  database <- "dice_forecast_rdata"
  file_id <- ""
  error <- str_pad(" ", 250)	
  
  rtn1 <-.C("gridfsControl", 
            command = as.character(command), 
            host = as.character(host), 
            user = as.character(user), 
            pw = as.character(pw), 
            port = as.integer(port), 
            database = as.character(database), 
            collection = as.character(collection), 
            targetdir = as.character(targetdir),
            filename = as.character(filename), 
            file_id = "", 
            error = as.character(error)
  )
  return(rtn1)
}

# List files in MondoDB collection.  This will print to Standard IO.  It was intended to return a character vector of filenames, but does not work.
ListMongo <- function(collection=NULL) {
  command <- "list"
  host <- "dataservices-mongodb.bsvecosystem.net"
  user <- "bsve_at_06496"
  pw <- "CfdHxw53"
  port <- 27017
  database <- "dice_forecast_rdata"
  file_id <- ""
  error <- str_pad(" ", 250)	
  
  rtn1 <-.C("gridfsControl", 
            command = as.character(command), 
            host = as.character(host), 
            user = as.character(user), 
            pw = as.character(pw), 
            port = as.integer(port), 
            database = as.character(database), 
            collection = as.character(collection), 
            targetdir = as.character(""),
            filename = as.character("file_list"), 
            file_id = "", 
            error = as.character(error)
  )

  return(rtn1)
}





