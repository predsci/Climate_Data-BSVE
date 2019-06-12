
# Declare a local temporary directory for files to be saved
my_dir = "/home/source/temp/"
# Designate the location of the R-function files
Rfuns_dir = "/home/source/"
# load C-function library for accessing MongoDB
dyn.load("/home/source/mongodbFunctions.so")

library(stringr)
library(curl)
library(DBI)
library(RMySQL)
library(RPostgreSQL)
library(ncdf4)    # for opening .nc NOAA files
library(lubridate)

# load R-functions
source(paste0(Rfuns_dir, "MongoFuns.R"))
source(paste0(Rfuns_dir, "NOAA_Funs.R"))

# Update mongo files from NOAA ftp
Check_NOAA_FTP(local_dir=my_dir)

# Update noaa_daily table from mongo files
UpdateNOAA_SQL_daily(local_dir=my_dir)


# Update noaa_xxxx (cadence) tables from noaa_daily
UpdateNOAA_SQL_ByCad()

# clean-up my_dir?

