
# load mongo functions
source("~/Dropbox/MyLEPR/SQL/BSVE/containers/docker-NOAA_proc/source/MongoFuns.R")
# load NOAA functions that include MySQL connection OpenCon()
source("~/Dropbox/MyLEPR/SQL/BSVE/containers/docker-NOAA_proc/source/NOAA_Funs.R")

# directory where NOAA .nc files reside
NOAA_dir = "/Users/turtle/Dropbox/MyLEPR/SQL/DataUpdate/NOAA_4x/"

# copy files to mongo
for(metric in c("sh", "precip", "temp")) {
  collection = paste0("NOAA_dat_", metric)
  subdir     = paste0("NOAA_", metric, "_original_files/")
  files      = list.files(path=paste0(NOAA_dir, subdir))
  
  for (filename in files) {
    cat("Writing ", filename, " to BSVE mongodb...", sep="")
    result = WriteToMongo(collection=collection, filename=filename, targetdir=paste0(NOAA_dir, subdir))
    cat("Complete \n")
  }
}


# Open connection to MySQL db
myDB = OpenCon()
# create noaa_mongo_files table
dbExecute(myDB, statement = "CREATE TABLE noaa_mongo_files (
                                id SERIAL PRIMARY KEY,
                                filename VARCHAR(50),
                                collection VARCHAR(50),
                                file_id VARCHAR(50),
                                last_update DATE
);")

# index NOAA data files in noaa_mongo_files
for(metric in c("sh", "precip", "temp")) { 
  collection = paste0("NOAA_dat_", metric)
  subdir     = paste0("NOAA_", metric, "_original_files/")
  files      = list.files(path=paste0(NOAA_dir, subdir))
  
  temp_df = data.frame(filename=files, collection=collection, file_id=NA, last_update=Sys.Date())
  
  dbWriteTable(myDB, name="noaa_mongo_files", value=temp_df, row.names=F, append=T, overwrite=F)
}


# write Pop_map_NOAA2GADM.Rds to mongo
file_dir   = "~/Dropbox/LEPR03/data/climate/"

result = WriteNOAAmap(targetdir=file_dir)

temp_df = data.frame(filename="Pop_map_NOAA2GADM.Rds", collection="dice_data_aux", file_id=NA, last_update=Sys.Date())
dbWriteTable(myDB, name="noaa_mongo_files", value=temp_df, row.names=F, append=T, overwrite=F)


dbDisconnect(myDB)








