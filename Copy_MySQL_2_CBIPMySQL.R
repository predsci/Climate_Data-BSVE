
rm(list=ls())

# require(DICE)
require(dplyr)
require(lubridate)
require(RMySQL)
# require(RPostgreSQL)

# load functions for opening connections
# source("~/Dropbox/MyLEPR/SQL/BSVE/containers/docker-NOAA_proc/source/NOAA_Funs.R")
source("/home/source/NOAA_Funs.R")

MySQLdb = OpenCon("predsci")
# CBIP_db = OpenCon("dcs_aws")
CBIP_db = OpenCon("cbip")


updt_dep_table <- function(table_name=NULL, ref_cols=NULL, data_cols=NULL, table_data=NULL, table_data2=NULL, P_SQLdb=NULL) {
  # determine rows of table_data that are not in table_data2
  append_rows = anti_join(x=table_data, y=table_data2, by=ref_cols)
  if (nrow(append_rows)>0) {
    # append to database
    dbWriteTable(CBIP_db, name=table_name, value=append_rows, row.names=FALSE, overwrite=F, append=T)
  }
  
  # determine rows that overlap (and keep data cols from table_data)
  update_rows  = semi_join(x=table_data, y=table_data2, by=ref_cols)
  # map overlap rows
  outer_mat    = matrix(TRUE, nrow=nrow(update_rows), ncol=nrow(table_data2))
  for (var in ref_cols) {
    outer_mat = outer_mat & outer(update_rows[[var]], table_data2[[var]], "==")
  }
  match_ind = which(outer_mat, arr.ind=T)
  # test rows for equality
  for (ii in 1:nrow(match_ind)) {
    is_equal = update_rows[match_ind[ii, 1], data_cols] == table_data2[match_ind[ii, 2], data_cols]
    if (any(!is_equal)) {
      write_vars = update_rows[match_ind[ii, 1], data_cols[!is_equal]]
      char_vars  = unlist(lapply(write_vars, FUN=is.character))
      write_vars[char_vars] = paste0("'", write_vars[char_vars], "'")
      set_char = paste0(data_cols[!is_equal], " = ", write_vars)
      set_char = paste(set_char, collapse=", ")
      where_vars = update_rows[match_ind[ii, 1], ref_cols]
      char_vars  = unlist(lapply(where_vars, FUN=is.character))

      nchar_vars = sum(char_vars)
      if (nchar_vars>0) {
        jj = which(char_vars)[1]
        where_char = paste0(ref_cols[jj], " = '", where_vars[jj], "'")
        if (nchar_vars>1) {
          for (jj in which(char_vars)[2:nchar_vars]) {
            where_char = paste0(where_char, " AND ", ref_cols[jj], " = '", where_vars[jj], "'")
          }
        }
      }
      
      nnum_vars = sum(!char_vars)
      if (nnum_vars>0) {
        jj = which(!char_vars)[1]
        if (nchar_vars>0) {
          where_char = paste0(where_char, " AND ", ref_cols[jj], " = ", where_vars[jj])
        } else {
          where_char = paste0(ref_cols[jj], " = ", where_vars[jj])
        }
        if (nnum_vars>1) {
          for (jj in which(!char_vars)[2:nnum_vars]) {
            where_char = paste0(where_char, " AND ", ref_cols[jj], " = ", where_vars[jj])
          }
        }
      }
      dbExecute(conn=CBIP_db, statement=paste0("UPDATE ", table_name ," SET ", set_char, " WHERE ", where_char, ";"))
    }
  }
}




table_name = "data_sources"
cat("Updating table ", table_name, ". \n", sep="")
ref_cols   = c("source_key", "cadence", "disease")
data_cols  = c("source", "source_abbv", "source_desc", "data_cols", "col_names", "col_units", "bsve_use", "countries", "max_lev")
table_data = dbReadTable(MySQLdb, table_name)
# table_data$bsve_use = as.logical(table_data$bsve_use)
# remove Quidel entry
table_data = table_data[table_data$source_abbv!="quidel", ]
if (!dbExistsTable(CBIP_db, table_name)) {
  # Create and populate table
  dbExecute(conn=CBIP_db, statement=paste0("CREATE TABLE ",table_name ," (
                                           source_key SMALLINT PRIMARY KEY,
                                           cadence SMALLINT NOT NULL,
                                           disease VARCHAR(50),
                                           source TEXT,
                                           source_abbv VARCHAR(50),
                                           source_desc VARCHAR(255),
                                           data_cols SMALLINT,
                                           col_names VARCHAR(255),
                                           col_units VARCHAR(255),
                                           bsve_use TINYINT,
                                           countries VARCHAR(765),
                                           max_lev SMALLINT,
                                           min_lev SMALLINT
  );"))
dbWriteTable(CBIP_db, name=table_name, value=table_data, row.names=FALSE, overwrite=F, append=T)
} else {
  # read existing table
  table_data2 = dbReadTable(CBIP_db, table_name)
  # perform table update
  updt_dep_table(table_name=table_name, ref_cols=ref_cols, data_cols=data_cols, table_data=table_data, table_data2=table_data2, P_SQLdb=CBIP_db)
}


table_name = "clim_by_disease"
cat("Updating table ", table_name, ". \n", sep="")
ref_cols   = c("disease")
data_cols  = c("n_clim", "clim_names")
table_data = dbReadTable(MySQLdb, table_name)
clim_by_disease = table_data
if (!dbExistsTable(CBIP_db, table_name)) {
  # dbExecute(conn=CBIP_db, statement=paste0("DROP TABLE IF EXISTS ", table_name))
  dbExecute(conn=CBIP_db, statement=paste0("CREATE TABLE ",table_name ," (
          disease VARCHAR(15) PRIMARY KEY,
          n_clim SMALLINT NOT NULL,
          clim_names VARCHAR(50)
);"))
  dbWriteTable(CBIP_db, name=table_name, value=table_data, row.names=FALSE, overwrite=F, append=T)
} else {
  # read existing table
  table_data2 = dbReadTable(CBIP_db, table_name)
  # perform table update
  updt_dep_table(table_name=table_name, ref_cols=ref_cols, data_cols=data_cols, table_data=table_data, table_data2=table_data2, P_SQLdb=CBIP_db)
}



table_name = "transmission_names"
cat("Updating table ", table_name, ". \n", sep="")
ref_cols   = c("trans_abbv")
data_cols  = c("trans_desc")
table_data = dbReadTable(MySQLdb, table_name)
if (!dbExistsTable(CBIP_db, table_name)) {
  dbExecute(conn=CBIP_db, statement=paste0("CREATE TABLE ",table_name ," (
                                         trans_abbv VARCHAR(5) PRIMARY KEY,
                                         trans_desc VARCHAR(50) NOT NULL
);"))
  dbWriteTable(CBIP_db, name=table_name, value=table_data, row.names=FALSE, overwrite=F, append=T)
} else {
  # read existing table
  table_data2 = dbReadTable(CBIP_db, table_name)
  # perform table update
  updt_dep_table(table_name=table_name, ref_cols=ref_cols, data_cols=data_cols, table_data=table_data, table_data2=table_data2, P_SQLdb=CBIP_db)
}



table_name = "disease_transmission"
cat("Updating table ", table_name, ". \n", sep="")
ref_cols   = c("dtype_id", "disease_abbv")
data_cols  = c("disease_desc", "trans_abbv")
table_data = dbReadTable(MySQLdb, table_name)
diseases = table_data$disease_abbv
if (!dbExistsTable(CBIP_db, table_name)) {
  dbExecute(conn=CBIP_db, statement=paste0("CREATE TABLE ",table_name ," (
                                         dtype_id SERIAL,
                                         disease_abbv VARCHAR(15) UNIQUE NOT NULL,
                                         disease_desc VARCHAR(50) NOT NULL,
                                         trans_abbv VARCHAR(5) REFERENCES transmission_names (trans_abbv),
                                         ui_name VARCHAR(30),
                                         PRIMARY KEY (dtype_id, disease_abbv)
);"))
  dbWriteTable(CBIP_db, name=table_name, value=table_data, row.names=FALSE, overwrite=F, append=T)
} else {
  # read existing table
  table_data2 = dbReadTable(CBIP_db, table_name)
  # perform table update
  updt_dep_table(table_name=table_name, ref_cols=ref_cols, data_cols=data_cols, table_data=table_data, table_data2=table_data2, P_SQLdb=CBIP_db)
}


table_name = "unit_types"
cat("Updating table ", table_name, ". \n", sep="")
ref_cols   = c("unit_key", "unit_type")
data_cols  = c("factor", "aggregate_method")
table_data = dbReadTable(MySQLdb, table_name)
if (!dbExistsTable(CBIP_db, table_name)) {
  dbExecute(conn=CBIP_db, statement=paste0("CREATE TABLE ",table_name ," (
                                           unit_key serial PRIMARY KEY,
                                           unit_type VARCHAR(50) NOT NULL,
                                           factor VARCHAR(200),
                                           aggregate_method VARCHAR(25),
                                           UNIQUE INDEX (unit_type)
  );"))
  dbWriteTable(CBIP_db, name=table_name, value=table_data, row.names=FALSE, overwrite=F, append=T)
} else {
  # read existing table
  table_data2 = dbReadTable(CBIP_db, table_name)
  # perform table update
  updt_dep_table(table_name=table_name, ref_cols=ref_cols, data_cols=data_cols, table_data=table_data, table_data2=table_data2, P_SQLdb=CBIP_db)
}


table_name = "col_units"
cat("Updating table ", table_name, ". \n", sep="")
ref_cols   = c("col_key", "col_unit")
data_cols  = c("unit_type")
table_data = dbReadTable(MySQLdb, table_name)
if (!dbExistsTable(CBIP_db, table_name)) {
  dbExecute(conn=CBIP_db, statement=paste0("CREATE TABLE ",table_name ," (
                                           col_key SERIAL PRIMARY KEY,
                                           col_unit VARCHAR(50) NOT NULL,
                                           unit_type VARCHAR(50) NOT NULL,
                                           FOREIGN KEY (unit_type) REFERENCES unit_types(unit_type)
  );"))
  dbWriteTable(CBIP_db, name=table_name, value=table_data, row.names=FALSE, overwrite=F, append=T)
} else {
  # read existing table
  table_data2 = dbReadTable(CBIP_db, table_name)
  # perform table update
  updt_dep_table(table_name=table_name, ref_cols=ref_cols, data_cols=data_cols, table_data=table_data, table_data2=table_data2, P_SQLdb=CBIP_db)
}


table_name = "season_se_dates"
cat("Updating table ", table_name, ". \n", sep="")
ref_cols   = c("abbv_1", "abbv_2", "season", "disease", "cadence")
data_cols  = c("start_date", "end_date")
table_data = dbReadTable(MySQLdb, table_name)
# names(table_data) = tolower(names(table_data))
if (!dbExistsTable(CBIP_db, table_name)) {
  dbExecute(conn=CBIP_db, statement=paste0("CREATE TABLE ",table_name ," (
                                         ABBV_1 CHAR(2) NOT NULL,
                                         ABBV_2 VARCHAR(10) NOT NULL,
                                         season SMALLINT NOT NULL,
                                         disease VARCHAR(15) REFERENCES disease_transmission (disease_abbv),
                                         cadence SMALLINT NOT NULL,
                                         start_date DATE NOT NULL,
                                         end_date DATE NOT NULL,
                                         PRIMARY KEY (ABBV_1, ABBV_2, season, disease, cadence)
);"))
  dbWriteTable(CBIP_db, name=table_name, value=table_data, row.names=FALSE, overwrite=F, append=T)
} else {
  # read existing table
  table_data2 = dbReadTable(CBIP_db, table_name)
  # perform table update
  updt_dep_table(table_name=table_name, ref_cols=ref_cols, data_cols=data_cols, table_data=table_data, table_data2=table_data2, P_SQLdb=CBIP_db)
}



for (disease in diseases) {
  # update incidence data
    # The data itself can be updated in weird ways, with cases being added or deleted at various intervals after the initial report.  For this reason, we simply truncate and re-write the entire table.
  table_name = paste0(disease, "_data")
  cat("Updating table ", table_name, ". \n", sep="")
  table_data = dbReadTable(MySQLdb, table_name)
  if (dbExistsTable(conn=CBIP_db, name=table_name)) {
    dbExecute(conn=CBIP_db, statement=paste0("TRUNCATE TABLE ", table_name))
    dbWriteTable(CBIP_db, name=table_name, value=table_data, row.names=FALSE, overwrite=F, append=T)
  } else {
    # clim_cols = strsplit(clim_by_disease$clim_names[clim_by_disease$disease==disease], ";")[[1]]
    data_cols = sum(substr(names(table_data), start=1,stop=4)=="data")
    dbExecute(conn=CBIP_db, statement=paste0("CREATE TABLE ",table_name ," (
                                             master_key CHAR(8) NOT NULL,
                                             source_key SMALLINT NOT NULL,
                                             date DATE NOT NULL, ",
                                             paste0("data", 1:data_cols, " REAL", collapse=", "),
                                             ", PRIMARY KEY (master_key, source_key, date)
    );"))
    dbWriteTable(CBIP_db, name=table_name, value=table_data, row.names=FALSE, overwrite=F, append=T)
  }
  
  
  
  
  table_name = paste0(disease, "_lut")
  table_data = dbReadTable(MySQLdb, table_name)
  # names(table_data) = tolower(names(table_data))
  if (dbExistsTable(conn=CBIP_db, name=table_name)) {
    dbExecute(conn=CBIP_db, statement=paste0("TRUNCATE TABLE ", table_name))
    dbWriteTable(CBIP_db, name=table_name, value=table_data, row.names=FALSE, overwrite=F, append=T)
  } else {
    spatial_levels = sum(substr(names(table_data), start=1, stop=4)=="name")
    dbExecute(conn=CBIP_db, statement=paste0("CREATE TABLE ",table_name ," (
                                           identifier VARCHAR(50) PRIMARY KEY,
                                           level SMALLINT NOT NULL,
                                           ", paste0("NAME_", 1:spatial_levels, " VARCHAR(50) \n", collapse=", "),", 
                                           ", paste0("ID_", 1:spatial_levels, " REAL \n", collapse=", "),",
                                           ", paste0("ABBV_", 1:spatial_levels, " VARCHAR(10) \n", collapse=", "),",
                                           inc_key INTEGER NOT NULL,
                                           master_key CHAR(8) UNIQUE NOT NULL,
                                           gadm_name VARCHAR(50),
                                           gadm_lvl SMALLINT,
                                           clim_ident VARCHAR(50) NOT NULL,
                                           gadm_noaa_sedac_ident VARCHAR(30),
                                           gadm_lat REAL,
                                           gadm_lon REAL,
                                           gadm_area REAL,
                                           sedac_lat REAL,
                                           sedac_lon REAL,
                                           sedac_pop INT
  );"))
    # Also create index on master_key.  Additional indices.....?
    dbExecute(conn=CBIP_db, statement=paste0("CREATE UNIQUE INDEX ", disease,"_key_idx ON ",table_name ," (master_key)"))
    dbWriteTable(CBIP_db, name=table_name, value=table_data, row.names=FALSE, overwrite=F, append=T)
  }
}




# school schedule tables
for (cadence in c("daily", "weekly", "monthly")) {
  table_name = paste0("school_", cadence)
  cat("Updating table ", table_name, ". \n", sep="")
  table_data = dbReadTable(MySQLdb, table_name)
  if (dbExistsTable(conn=CBIP_db, name=table_name)) {
    dbExecute(conn=CBIP_db, statement=paste0("TRUNCATE TABLE ", table_name))
    dbWriteTable(CBIP_db, name=table_name, value=table_data, row.names=FALSE, overwrite=F, append=T)
  } else {
    dbExecute(conn=CBIP_db, statement=paste0("CREATE TABLE school_weekly (
                                     master_key CHAR(8) NOT NULL,
                                     date DATE NOT NULL,
                                     school REAL,
                                     PRIMARY KEY (master_key, date)
    );"))
    dbWriteTable(CBIP_db, name=table_name, value=table_data, row.names=FALSE, overwrite=F, append=T)
  }
}


# population table
table_name = "pop_yearly"
cat("Updating table ", table_name, ". \n", sep="")
ref_cols   = c("master_key", "date")
data_cols  = c("pop")
table_data = dbReadTable(MySQLdb, table_name)
if (!dbExistsTable(CBIP_db, table_name)) {
  dbExecute(conn=CBIP_db, statement=paste0("CREATE TABLE ",table_name ," (
                                           master_key CHAR(8) NOT NULL,
                                           date DATE NOT NULL,
                                           pop INT NULL,
                                           PRIMARY KEY (master_key, date)
  );"))
dbWriteTable(CBIP_db, name=table_name, value=table_data, row.names=FALSE, overwrite=F, append=T)
} else {
  dbExecute(conn=CBIP_db, statement=paste0("TRUNCATE TABLE ", table_name))
  dbWriteTable(CBIP_db, name=table_name, value=table_data, row.names=FALSE, overwrite=F, append=T)
}


# season onset
table_name = "season_onset"
cat("Updating table ", table_name, ". \n", sep="")
ref_cols   = c("master_key", "year")
data_cols  = c("onset")
table_data = dbReadTable(MySQLdb, table_name)
if (!dbExistsTable(CBIP_db, table_name)) {
  dbExecute(conn=CBIP_db, statement=paste0("CREATE TABLE ",table_name ," (
                                           master_key CHAR(8) NOT NULL,
                                           year INT NOT NULL,
                                           onset REAL NULL,
                                           PRIMARY KEY (master_key, year)
  );"))
dbWriteTable(CBIP_db, name=table_name, value=table_data, row.names=FALSE, overwrite=F, append=T)
} else {
  dbExecute(conn=CBIP_db, statement=paste0("TRUNCATE TABLE ", table_name))
  dbWriteTable(CBIP_db, name=table_name, value=table_data, row.names=FALSE, overwrite=F, append=T)
}



dbDisconnect(conn=CBIP_db)

dbDisconnect(MySQLdb)
