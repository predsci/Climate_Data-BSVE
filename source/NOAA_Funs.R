
# Check NOAA ftp site for updated climate-data files. If new(er) files exist, download and write(replace) MongoDB files. 
Check_NOAA_FTP <- function(local_dir=NULL) {
  # Check NOAA ftp site for updated files.  If new files exist, download them.
  ftp_path = "ftp://ftp.cdc.noaa.gov/Datasets/ncep.reanalysis2/gaussian_grid/"
  
  # file_types and local_path must have matching entries
  bsve_con = OpenCon()
  noaa_files = dbReadTable(bsve_con, name="noaa_mongo_files")
  dbDisconnect(bsve_con)
  
  noaa_ind = substr(noaa_files$collection, start=1,stop=8)=="NOAA_dat"
  
  unique_files = noaa_files[noaa_ind, c("filename", "collection")]
  # find files with XXXX.nc ending (NOAA data files)
  unique_files$filename = gsub(pattern="(\\d){4}\\.nc$", replacement="", x=unique_files$filename, fixed=F)
  unique_files = unique(unique_files)
  # determine which file types (climate metrics) need to be updated
  file_types = unique_files$filename
  
  # file_types = c("air.2m.gauss.", "shum.2m.gauss.", "prate.sfc.gauss.")
  # local_path = c("~/Dropbox/MyLEPR/SQL/DataUpdate/NOAA_4x/NOAA_temp_original_files/", 
  #                "~/Dropbox/MyLEPR/SQL/DataUpdate/NOAA_4x/NOAA_sh_original_files/", 
  #                "~/Dropbox/MyLEPR/SQL/DataUpdate/NOAA_4x/NOAA_precip_original_files/")
  
  
  con = curl(ftp_path)
  ftp_file_details = readLines(con)
  close(con)
  
  for (ii in 1:length(file_types)) {
    # determine most recent local file
    # get filenames
    # local_files = list.files(path=local_path[ii])
    local_file_ind = substr(x=noaa_files$filename, start=1, stop=nchar(file_types[ii]))==file_types[ii]
    local_files = noaa_files$filename[local_file_ind]
    # extract file year
    local_years = as.integer(substr(local_files, start=nchar(file_types[ii])+1, stop=nchar(file_types[ii])+4))
    # determine max year
    local_max_year = local_years[which.max(local_years)]
    local_max_name = local_files[which.max(local_years)]
    # get modified date
    # finfo = file.info(paste0(local_path[ii], local_max_name))
    # local_timestamp = finfo$mtime
    local_date = noaa_files$last_update[noaa_files$filename==local_max_name]
    
    # determine most recent ftp file
    ftp_ind = grep(pattern=file_types[ii], x=ftp_file_details, fixed=T)
    
    ftp_names = character(length(ftp_ind))
    for (jj in 1:length(ftp_ind)) {
      ftp_names[jj] = word(string=ftp_file_details[ftp_ind[jj]], start=-1)
    }
    ftp_years = as.integer(substr(ftp_names, start=nchar(file_types[ii])+1, stop=nchar(file_types[ii])+4))
    
    ftp_max_year = max(ftp_years)
    ftp_max_name = ftp_names[which.max(ftp_years)]
    ftp_max_ind  = ftp_ind[which.max(ftp_years)]
    
    # determine which year-files to download
    down_years = integer()
    if (ftp_max_year>local_max_year) {
      # download all years local_max_year<=years<=ftp_max_year
      down_years = local_max_year:ftp_max_year
    } else {
      # compare ftp and local timestamps
      # extract ftp date-time info
      date_info = word(string=ftp_file_details[ftp_max_ind], start=6, end=8)
      line_split = str_split(string=ftp_file_details[ftp_max_ind], pattern=" ")
      line_split = line_split[[1]]
      line_split = line_split[line_split!=""]
      date_info  = line_split[6:8]
      
      # ftp_timestamp = strptime(x=paste0(ftp_max_year, "-", date_info[1], "-", date_info[2], " ", date_info[3]), format="%Y-%b-%d %H:%M")
      ftp_date = as.Date(x=paste0(ftp_max_year, "-", date_info[1], "-", date_info[2]), format="%Y-%b-%d")
      
      # if (ftp_timestamp>local_timestamp) {
      #   down_years = ftp_max_year
      # }
      if (ftp_date>=local_date) {
        down_years = ftp_max_year
      }
    }
    
    if (length(down_years)>0) {
      for (jj in 1:length(down_years)) {
        file_jj = paste0(file_types[ii], down_years[jj], ".nc")
        cat("Downloading file ", file_jj, ".\n", sep="")
        ftp_temp = paste0(ftp_path, file_jj)
        # local_temp = paste0(local_path[ii], file_types[ii], down_years[jj], ".nc")
        local_temp = paste0(local_dir, file_jj)
        # download to working directory
        curl_download(url=ftp_temp, destfile=local_temp)
        # remove old file from mongo
        cat("Removing existing copy in MongoDB.\n")
        remove_out = RemoveFromMongo(collection=unique_files$collection[ii], filename=file_jj, targetdir="")
        # upload to mongo
        cat("Writing ", file_jj, " to MondoDB.\n", sep="")
        write_out = WriteToMongo(collection=unique_files$collection[ii], filename=file_jj, targetdir=paste0(getwd()))
        # update the date in noaa_mongo_files
        bsve_con = OpenCon()
        cat("Updating 'noaa_mongo_files' table in PostGreSQL")
        dbExecute(con=bsve_con, statement=paste0("UPDATE noaa_mongo_files SET last_update='", Sys.Date(), "', file_id='", write_out$file_id, "' WHERE filename='", file_jj, "';"))
        dbDisconnect(bsve_con)
      } 
    } else {
      # local files are up-to-date. do nothing
      cat("MongoDB ", file_types[ii], "nc files are up-to-date.\n", sep="")
    }
  }
}


UpdateNOAA_SQL_daily <- function(local_dir="/Users/turtle/") {
  
  # DataDir = "~/Dropbox/MyLEPR/SQL/DataUpdate/NOAA_4x/" # NOAA climate data
  
  # Read NOAA -> master_key map from MongoDB
  read_out = ReadNOAA_map(targetdir=local_dir)
  NOAA_SEDAC_map = readRDS(file=paste0(local_dir, read_out$filename))
  
  # SQL connection
  myDB = OpenCon()
  
  # Determine years available in NOAA data files
  files_table = dbReadTable(myDB, "noaa_mongo_files")
  NOAA_files  = files_table$filename[substr(files_table$collection, start=1, stop=4)=="NOAA"]
  NOAA_files_sh = NOAA_files[substr(NOAA_files, start=1, stop=4)=="shum"]
  
  NOAA_years = as.integer(substr(NOAA_files_sh, start=15, stop=18))
  NOAA_years = sort(unique(NOAA_years))
  
  source_table = dbReadTable(myDB, name="data_sources")
  diseases = unique(as.character(source_table$disease))
  
  if (!dbExistsTable(myDB, name="noaa_daily")) {
    dbExecute(conn=myDB, statement=paste0("CREATE TABLE noaa_daily (
                                          master_key CHAR(8) NOT NULL,
                                          date DATE NOT NULL,
                                          sh FLOAT,
                                          precip FLOAT,
                                          temp FLOAT,
                                          INDEX (master_key, date)
    )"))
  }
  
  # determine all incidence data available and create a list of gadm identifiers needed for climate data
  master_list = data.frame(master_key=character(), gadm_ident=character(), clim_ident=character(), stringsAsFactors=F)
  master_lut = list(length(diseases))
  child_agg = list()
  agg_idents = data.frame(ident=character(), master_key=character(), stringsAsFactors=F)
  for (ii in 1:length(diseases)) {
    disease = diseases[ii]
    
    # download disease lookup table
    if (!dbExistsTable(myDB, paste0(disease,"_lut"))) {
      next
    }
    temp_lut = dbReadTable(myDB, paste0(disease,"_lut"))
    maxlevel = max(as.integer(substr(names(temp_lut[substr(names(temp_lut),start=1,stop=4)=="abbv"]), start=6, stop=7)))
    
    # append appropriate lut columns to master_list
    most_recent = temp_lut[, c("master_key", "gadm_noaa_sedac_ident", "clim_ident")]
    names(most_recent)[2] = "gadm_ident"
    master_list = rbind(master_list, most_recent)
    # remove duplicate rows
    master_list = unique(master_list)
    
    # compile list of children for non-gadm regions
    # if !is.na(gadm_ident) add gadm_ident to list
    for (jj in 1:length(most_recent$master_key)) {
      if (is.na(most_recent$gadm_ident[jj])) {
        # use LUT to search for gadm children
        lut_ind = which(temp_lut$master_key==most_recent$master_key[jj])
        level = temp_lut$level[lut_ind]
        if (level < maxlevel) {
          child_ind = temp_lut$NAME_2==temp_lut$NAME_2[lut_ind] & temp_lut$level>level
          if (level>2) {
            for (lev in 3:level) {
              child_ind = child_ind & temp_lut[lut_ind, paste0("name_",lev)]==temp_lut[, paste0("name_",lev)]
            }
            child_ind[is.na(child_ind)] = FALSE
          }
          if (any(child_ind)) {
            max_child_lev = max(temp_lut$level[child_ind])
            for (lev in (level+1):max_child_lev) {
              child_lev_ind = child_ind & temp_lut$level==lev
              if(all(!is.na(temp_lut$gadm_noaa_sedac_ident[child_lev_ind]))) {
                master_key = temp_lut$master_key[lut_ind]
                if (!(master_key %in% names(child_agg)))
                  # record child weights for aggregating the climate data
                  child_agg[[master_key]] = list(agg=TRUE, children=data.frame(child=temp_lut$gadm_noaa_sedac_ident[child_lev_ind], weights=temp_lut$sedac_pop[child_lev_ind]/sum(temp_lut$sedac_pop[child_lev_ind]), stringsAsFactors=F))
                agg_idents = rbind(agg_idents, data.frame(ident=temp_lut$gadm_noaa_sedac_ident[child_lev_ind], master_key=temp_lut$master_key[child_lev_ind], stringsAsFactors=F))
                break
              }
            }
          }
        }
      }
    }
    # else look for and add gadm children idents to list
    
  }
  
  
  # first we will update existing keys -----------------------------------------------
  cat("Update existing master_keys. \n")
  existing_keys = dbGetQuery(myDB, statement="SELECT DISTINCT(master_key) FROM noaa_daily;")
  existing_ind = master_list$master_key %in% existing_keys$master_key
  existing_agg = agg_idents$master_key %in% existing_keys$master_key
  
  gadm_idents = master_list$gadm_ident[!is.na(master_list$gadm_ident) & existing_ind]
  gadm_idents = unique(c(gadm_idents, agg_idents$ident))
  
  # reduce master_list to gadm_idents
  master_list_orig = master_list
  master_list = master_list[existing_ind, ]
  
  # Use NOAA_SEDAC_map to process the nesessary patches
  # source("~/Dropbox/MyLEPR/SQL/DataUpdate/NOAA_4x/Processing_Scripts/NOAA_PopDensity_funs.R")
  
  # determine years to process
  # determine max noaa_daily date
  max_date = dbGetQuery(myDB, statement = "SELECT MAX(date) FROM noaa_daily;")
  max_daily_date = as.Date(max_date[[1]])
  
  # determine max NOAA file date
  input_file=paste("shum.2m.gauss.",NOAA_years[length(NOAA_years)],".nc",sep = "")
  
  # download file
  read_out = ReadFromMongo(filename=input_file, collection="NOAA_dat_sh", targetdir=local_dir)
  # open file
  tmp.dat = OpenNOAAyear(dname="shum", file=paste0(local_dir, input_file))
  max_file_date = max(as.Date(names(tmp.dat)[3:length(tmp.dat)]))
  if (max_file_date > max_daily_date) {
    years = year(max_daily_date):year(max_file_date)
  } else {
    years = NULL
  }
  
  dbDisconnect(myDB)
  # !!!!!!!!!!!!!! Do not leave this line in the code !!!!!!!!!!!!!!!!!
  stop("Testing.  Stop before changes are made. \n")
  # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
  if (is.null(years)) {
    cat("Existing keys are up-to-date. Skipping to new keys. \n")
  } else {
    
    start.time = proc.time()
    NOAA_sh = NULL
    for(year in years){
      cat("processing SH for year ",year,"\n")
      
      # Process SH
      dname<-"shum"
      # input_path=paste(DataDir,"NOAA_sh_original_files/shum.2m.gauss.",year,".nc",sep = "")
      # # Open the appropriate NOAA file for one year
      # tmp.dat = OpenNOAAyear(file=input_path, dname=dname)
      input_file=paste("shum.2m.gauss.",year,".nc",sep = "")
      # download file
      read_out = ReadFromMongo(filename=input_file, collection="NOAA_dat_sh", targetdir=local_dir)
      # open file
      tmp.dat = OpenNOAAyear(dname=dname, file=paste0(local_dir, input_file))
      
      
      # if max_daily_date is in this year, chop tmp.dat
      if (year(max_daily_date)==year) {
        if (month(max_daily_date)==12 & mday(max_daily_date)==31) {
          # this year does not need to be processed
          next
        } else {
          col_ind = which(as.Date(names(tmp.dat)[3:length(tmp.dat)])==max_daily_date)
          tmp.dat = tmp.dat[, c(1,2,(3+col_ind):length(tmp.dat))]
        }
      }
      
      # Process to individual administrative areas
      result = NOAA2adm_area_idents(NOAA.dat=tmp.dat, NOAA_SEDAC_map=NOAA_SEDAC_map, idents=gadm_idents)
      # Combine years
      if(is.null(NOAA_sh)) {
        NOAA_sh = result
      } else {
        NOAA_sh = rbind(NOAA_sh, result)
      }
    }
    
    cat(proc.time() - start.time,"\n\n")
    
    
    # Loop over years for Precip
    NOAA_precip = NULL
    for(year in years){
      cat("processing Precip for year ",year,"\n")
      
      # Process precip
      dname<-"prate"
      # input_path=paste(DataDir,"NOAA_precip_original_files/prate.sfc.gauss.",year,".nc",sep = "")
      # # Open the appropriate NOAA file for one year
      # tmp.dat = OpenNOAAyear(file=input_path, dname=dname)
      input_file=paste("prate.sfc.gauss.",year,".nc",sep = "")
      # download file
      read_out = ReadFromMongo(filename=input_file, collection="NOAA_dat_precip", targetdir=local_dir)
      # open file
      tmp.dat = OpenNOAAyear(dname=dname, file=paste0(local_dir, input_file))
      
      
      # if max_daily_date is in this year, chop tmp.dat
      if (year(max_daily_date)==year) {
        if (month(max_daily_date)==12 & mday(max_daily_date)==31) {
          # this year does not need to be processed
          next
        } else {
          col_ind = which(as.Date(names(tmp.dat)[3:length(tmp.dat)])==max_daily_date)
          tmp.dat = tmp.dat[, c(1,2,(3+col_ind):length(tmp.dat))]
        }
      }
      
      # Process to individual administrative areas
      result = NOAA2adm_area_idents(NOAA.dat=tmp.dat, NOAA_SEDAC_map=NOAA_SEDAC_map, idents=gadm_idents)
      # Combine years
      if(is.null(NOAA_precip)) {
        NOAA_precip = result
      } else {
        NOAA_precip = rbind(NOAA_precip, result)
      }
    }
    # scale to mm
    # kg/m^2/s^2 -> mm
    NOAA_precip[,3:length(NOAA_precip)]  = 60*60*24*NOAA_precip[,3:length(NOAA_precip)]
    
    
    
    # Loop over years for temperature
    NOAA_temp = NULL
    for(year in years){
      cat("processing temperature for year ",year,"\n")
      
      # Process temp
      dname<-"air"
      # input_path=paste(DataDir,"NOAA_temp_original_files/air.2m.gauss.",year,".nc",sep = "")
      # # Open the appropriate NOAA file for one year
      # tmp.dat = OpenNOAAyear(file=input_path, dname=dname)
      input_file=paste("air.2m.gauss.",year,".nc",sep = "")
      # download file
      read_out = ReadFromMongo(filename=input_file, collection="NOAA_dat_temp", targetdir=local_dir)
      # open file
      tmp.dat = OpenNOAAyear(dname=dname, file=paste0(local_dir, input_file))
      
      # if max_daily_date is in this year, chop tmp.dat
      if (year(max_daily_date)==year) {
        if (month(max_daily_date)==12 & mday(max_daily_date)==31) {
          # this year does not need to be processed
          next
        } else {
          col_ind = which(as.Date(names(tmp.dat)[3:length(tmp.dat)])==max_daily_date)
          tmp.dat = tmp.dat[, c(1,2,(3+col_ind):length(tmp.dat))]
        }
      }
      
      # Process to individual administrative areas
      result = NOAA2adm_area_idents(NOAA.dat=tmp.dat, NOAA_SEDAC_map=NOAA_SEDAC_map, idents=gadm_idents)
      # Combine years
      if(is.null(NOAA_temp)) {
        NOAA_temp = result
      } else {
        NOAA_temp = rbind(NOAA_temp, result)
      }
    }
    # convert to Celcius
    NOAA_temp[,3:length(NOAA_temp)] = NOAA_temp[,3:length(NOAA_temp)] - 273
    
    # clim_dates = as.Date(paste0(NOAA_sh$year,"-",NOAA_sh$day), format="%Y-%j")
    # calc dates based off of max_daily_date
    clim_dates = max_daily_date + (1:nrow(NOAA_sh))
    date_ind   = rep(TRUE, length(clim_dates))
    
    # pre-allocate data.frame
    NOAA_daily = data.frame(master_key=character(length(master_list$master_key)*length(clim_dates)), date=as.Date("2000-01-01"), sh=0, temp=0, precip=0, stringsAsFactors=FALSE)
    row_count = 1
    for (ii in 1:length(master_list$master_key)) {
      cat("Processing master_key ", master_list$master_key[ii],"\n", sep="")
      if (!is.na(master_list$gadm_ident[ii])) {
        # pull appropriate dates to SQL-form data frame
        # date_ind = clim_dates>=master_list$min_date[ii] & clim_dates<=master_list$max_date[ii]
        if (any(date_ind)) {
          temp_sh = NOAA_sh[date_ind, master_list$gadm_ident[ii]]
          temp_temp = NOAA_temp[date_ind, master_list$gadm_ident[ii]]
          temp_precip = NOAA_precip[date_ind, master_list$gadm_ident[ii]]
          new_rows = data.frame(master_key=master_list$master_key[ii], date=clim_dates[date_ind], sh=temp_sh, temp=temp_temp, precip=temp_precip, stringsAsFactors=FALSE)
        }
      } else {
        # Check to see if any non-gadm levels can be aggregated from child gadm patches
        if (master_list$master_key[ii] %in% names(child_agg)) {
          agg_weights = child_agg[[master_list$master_key[ii]]]$children
          # date_ind = clim_dates>=master_list$min_date[ii] & clim_dates<=master_list$max_date[ii]
          if (any(date_ind)) {
            temp_sh = NOAA_sh[date_ind, as.character(agg_weights$child)]
            temp_sh = as.matrix(temp_sh) %*% agg_weights$weights
            temp_temp = NOAA_temp[date_ind, as.character(agg_weights$child)]
            temp_temp = as.matrix(temp_temp) %*% agg_weights$weights
            temp_precip = NOAA_precip[date_ind, as.character(agg_weights$child)]
            temp_precip = as.matrix(temp_precip) %*% agg_weights$weights
            new_rows = data.frame(master_key=master_list$master_key[ii], date=clim_dates[date_ind], sh=temp_sh, temp=temp_temp, precip=temp_precip, stringsAsFactors=FALSE)
          }
        } else {
          # delete all rows of new rows
          new_rows = new_rows[0, ]
        }
      }
      if (nrow(new_rows)>0) {
        # record new values
        row_new = row_count + length(new_rows$master_key)
        NOAA_daily[row_count:(row_new-1), ] = new_rows
        row_count = row_new
      }
    }
    
    # remove extra rows of NOAA_daily
    NOAA_daily = NOAA_daily[1:(row_count-1), ]
    
    # re-open MySQL connection
    myDB = OpenCon()
    
    dbWriteTable(myDB, name="noaa_daily", value=NOAA_daily, append=TRUE, overwrite=FALSE, row.names=FALSE)
    
    dbDisconnect(myDB)
  }
  
  
  
  
  # next we will add new keys -------------------------------------------------
  
  cat("Process all years for new master_keys. \n")
  gadm_idents = master_list_orig$gadm_ident[!is.na(master_list_orig$gadm_ident) & !existing_ind]
  gadm_idents = unique(c(gadm_idents, as.character(agg_idents$ident)))
  if (length(gadm_idents)==0) {
    cat("No new master_keys to process.  Ending daily update.\n\n")
    return()
  }
  
  master_list = master_list_orig[!existing_ind, ]
  
  
  # determine years to process
  years = NOAA_years
  
  start.time = proc.time()
  NOAA_sh = NULL
  for(year in years){
    cat("processing SH for year ",year,"\n")
    
    # Process SH
    dname<-"shum"
    # input_path=paste(DataDir,"NOAA_sh_original_files/shum.2m.gauss.",year,".nc",sep = "")
    # # Open the appropriate NOAA file for one year
    # tmp.dat = OpenNOAAyear(file=input_path, dname=dname)
    # determine max NOAA file date
    input_file=paste("shum.2m.gauss.",year,".nc",sep = "")
    # download file
    read_out = ReadFromMongo(filename=input_file, collection="NOAA_dat_sh", targetdir=local_dir)
    # open file
    tmp.dat = OpenNOAAyear(dname=dname, file=paste0(local_dir, input_file))
    
    
    
    # Process to individual administrative areas
    result = NOAA2adm_area_idents(NOAA.dat=tmp.dat, NOAA_SEDAC_map=NOAA_SEDAC_map, idents=gadm_idents)
    # Combine years
    if(year==years[1]) {
      NOAA_sh = result
    } else {
      NOAA_sh = rbind(NOAA_sh, result)
    }
  }
  
  cat(proc.time() - start.time,"\n\n")
  
  
  # Loop over years for Precip
  NOAA_precip = NULL
  for(year in years){
    cat("processing Precip for year ",year,"\n")
    
    # Process precip
    dname<-"prate"
    # input_path=paste(DataDir,"NOAA_precip_original_files/prate.sfc.gauss.",year,".nc",sep = "")
    # # Open the appropriate NOAA file for one year
    # tmp.dat = OpenNOAAyear(file=input_path, dname=dname)
    input_file=paste("prate.sfc.gauss.",year,".nc",sep = "")
    # download file
    read_out = ReadFromMongo(filename=input_file, collection="NOAA_dat_precip", targetdir=local_dir)
    # open file
    tmp.dat = OpenNOAAyear(dname=dname, file=paste0(local_dir, input_file))
    
    # Process to individual administrative areas
    result = NOAA2adm_area_idents(NOAA.dat=tmp.dat, NOAA_SEDAC_map=NOAA_SEDAC_map, idents=gadm_idents)
    # Combine years
    if(year==years[1]) {
      NOAA_precip = result
    } else {
      NOAA_precip = rbind(NOAA_precip, result)
    }
  }
  # scale to mm
  # kg/m^2/s^2 -> mm
  NOAA_precip[,3:length(NOAA_precip)]  = 60*60*24*NOAA_precip[,3:length(NOAA_precip)]
  
  
  # Loop over years for temperature
  NOAA_temp = NULL
  for(year in years){
    cat("processing temperature for year ",year,"\n")
    
    # Process temp
    dname<-"air"
    # input_path=paste(DataDir,"NOAA_temp_original_files/air.2m.gauss.",year,".nc",sep = "")
    # # Open the appropriate NOAA file for one year
    # tmp.dat = OpenNOAAyear(file=input_path, dname=dname)
    input_file=paste("air.2m.gauss.",year,".nc",sep = "")
    # download file
    read_out = ReadFromMongo(filename=input_file, collection="NOAA_dat_temp", targetdir=local_dir)
    # open file
    tmp.dat = OpenNOAAyear(dname=dname, file=paste0(local_dir, input_file))
    
    # Process to individual administrative areas
    result = NOAA2adm_area_idents(NOAA.dat=tmp.dat, NOAA_SEDAC_map=NOAA_SEDAC_map, idents=gadm_idents)
    # Combine years
    if(year==years[1]) {
      NOAA_temp = result
    } else {
      NOAA_temp = rbind(NOAA_temp, result)
    }
  }
  # convert to Celcius
  NOAA_temp[,3:length(NOAA_temp)] = NOAA_temp[,3:length(NOAA_temp)] - 273
  
  clim_dates = as.Date(paste0(NOAA_sh$year,"-",NOAA_sh$day), format="%Y-%j")
  date_ind   = rep(TRUE, length(clim_dates))
  # pre-allocate data.frame
  NOAA_daily = data.frame(master_key=character(length(master_list$master_key)*length(clim_dates)), date=as.Date("2000-01-01"), sh=0, temp=0, precip=0, stringsAsFactors=FALSE)
  row_count = 1
  new_rows = data.frame(master_key=character(), date=character(), sh=numeric(), temp=numeric(), precip=numeric(), stringsAsFactors=FALSE)
  for (ii in 1:length(master_list$master_key)) {
    cat("Processing master_key ", master_list$master_key[ii],"\n", sep="")
    if (!is.na(master_list$gadm_ident[ii])) {
      # pull appropriate dates to SQL-form data frame
      # date_ind = clim_dates>=master_list$min_date[ii] & clim_dates<=master_list$max_date[ii]
      if (any(date_ind)) {
        temp_sh = NOAA_sh[date_ind, master_list$gadm_ident[ii]]
        temp_temp = NOAA_temp[date_ind, master_list$gadm_ident[ii]]
        temp_precip = NOAA_precip[date_ind, master_list$gadm_ident[ii]]
        new_rows = data.frame(master_key=master_list$master_key[ii], date=clim_dates[date_ind], sh=temp_sh, temp=temp_temp, precip=temp_precip, stringsAsFactors=FALSE)
      }
    } else {
      # Check to see if any non-gadm levels can be aggregated from child gadm patches
      if (master_list$master_key[ii] %in% names(child_agg)) {
        agg_weights = child_agg[[master_list$master_key[ii]]]$children
        # date_ind = clim_dates>=master_list$min_date[ii] & clim_dates<=master_list$max_date[ii]
        if (any(date_ind)) {
          temp_sh = NOAA_sh[date_ind, as.character(agg_weights$child)]
          temp_sh = as.matrix(temp_sh) %*% agg_weights$weights
          temp_temp = NOAA_temp[date_ind, as.character(agg_weights$child)]
          temp_temp = as.matrix(temp_temp) %*% agg_weights$weights
          temp_precip = NOAA_precip[date_ind, as.character(agg_weights$child)]
          temp_precip = as.matrix(temp_precip) %*% agg_weights$weights
          new_rows = data.frame(master_key=master_list$master_key[ii], date=clim_dates[date_ind], sh=temp_sh, temp=temp_temp, precip=temp_precip, stringsAsFactors=FALSE)
        }
      } else {
        # delete all rows of new rows
        new_rows = new_rows[0, ]
      }
    }
    if (nrow(new_rows)>0) {
      # record new values
      row_new = row_count + length(new_rows$master_key)
      NOAA_daily[row_count:(row_new-1), ] = new_rows
      row_count = row_new
    }
  }
  
  if (row_count>1) {
    # remove extra rows of NOAA_daily
    NOAA_daily = NOAA_daily[1:(row_count-1), ]
    
    # re-open MySQL connection
    myDB=OpenCon()
    # write new keys to noaa_daily 
    dbWriteTable(myDB, name="noaa_daily", value=NOAA_daily, append=TRUE, overwrite=FALSE, row.names=FALSE)
    
    dbDisconnect(myDB)
  } else {
    cat("No climate data generated for new patches.  Nothing written to MySQL.")
  }
  
  
  }


# Open a local NOAA .nc (6-hour) file and average to daily cadence. 
OpenNOAAyear <- function(dname=NULL, file=NULL) {
  #Loading and extracting File
  fid1  <- nc_open(file)
  lat   <- ncvar_get(fid1,"lat")
  lon   <- ncvar_get(fid1,"lon")
  time  <- ncvar_get(fid1,"time")
  # pull data values
  tmp.array <- ncvar_get(fid1,dname)
  fillvalue <- ncatt_get(fid1, dname, "_FillValue")
  nc_close(fid1)
  #Unpacking  
  tmp.array[tmp.array == fillvalue$value] <- NA
  t     = as.POSIXct(time*3600,origin='1800-01-01 00:00:0.0') 
  tmp.vec.long  <- as.vector(tmp.array)
  nlat  = dim(lat)
  nlong = dim(lon)
  nt    = dim(time)
  test  <- matrix(tmp.vec.long,nrow=nlat*nlong,ncol=nt)
  lonlat <- expand.grid(lon,lat)
  tmp.dat <- data.frame(cbind(lonlat, test))
  t     = as.character(t)
  names(tmp.dat) <- c("lon","lat",t)
  a     <- length(names(tmp.dat))
  tmp.dat = na.omit(tmp.dat)
  
  
  date_vec<- unique(substr(names(tmp.dat)[3:length(tmp.dat)],1,10))
  
  # Average 6-hourly to daily (4 at a time)
  for(ii in 1:length(date_vec)) {
    tmp.dat[,date_vec[ii]] = rowMeans(tmp.dat[,(4*ii-1):(4*ii+2)])
  }
  
  
  # Remove all 6-hour columns
  tmp.dat[,3:a]<-NULL
  
  return(tmp.dat)
}


# Process a year of NOAA data 'tmp.dat' to specific individual administrative areas.  This uses the NOAA_SEDAC_map to process the NOAA grid to population-weighted daily climate time-series.
NOAA2adm_area_idents <- function(NOAA.dat=NULL, NOAA_SEDAC_map=NULL, idents=NULL) {
  
  # create year, week, and day vectors
  dates = as.Date(names(NOAA.dat[,c(-1,-2)]))
  # if first day is from previous year, remove
  if (year(dates[1]) < year(dates[2])) {
    NOAA.dat = NOAA.dat[, -3]
    dates    = dates[-1]
  }
  days = 1:length(dates)
  year = rep(as.integer(format(dates[1],"%Y")), length(dates))
  result = data.frame(year=year, day=days)
  
  # determine all countries present in NOAA_SEDAC_map
  map_names = names(NOAA_SEDAC_map)
  countries = map_names[nchar(map_names)==2]
  
  # determine all countries present in idents
  ident_iso2 = substr(idents, start=1, stop=2)
  ident_countries = unique(ident_iso2)
  
  # for each country in NOAA_SEDAC_map, process all admin areas
  for(ii in 1:length(ident_countries)) {
    coords = NOAA_SEDAC_map[[ident_countries[ii]]]$NOAA_num
    # subset the NOAA grid. Include only points present in this country
    tmp.dat = NOAA.dat[coords,3:length(NOAA.dat)]
    
    identifiers = idents[ident_iso2==ident_countries[ii]]
    
    for(jj in 1:length(identifiers)) {
      weights = NOAA_SEDAC_map[[ident_countries[ii]]][[identifiers[jj]]]
      result[[identifiers[jj]]] = apply(X=tmp.dat, MARGIN=2, FUN=AverageRows, weights=weights)
    }
  }
  return(result)
}


# Update/Create weekly/monthly/yearly climate time-series as needed to match existing incidence data.  Create full history of climate data for historic analysis.
UpdateNOAA_SQL_ByCad <- function() {

  # SQL connection with BSVE PostGreSQL database
  myDB = OpenCon()
  
  source_table = dbReadTable(myDB, name="data_sources")
  diseases = unique(as.character(source_table$disease))
  
  # For each key in climate data, determine corresponding incidence cadence and pre-calc temporal averages.  Then insert into SQL incidence tables
  NOAA_keys  = dbGetQuery(myDB, statement="SELECT DISTINCT master_key FROM noaa_daily;")
  NOAA_keys  = NOAA_keys$master_key
  NOAA_dates = dbGetQuery(myDB, statement="SELECT DISTINCT date FROM noaa_daily;")
  NOAA_dates = as.Date(NOAA_dates$date)
  
  # determine first Sunday and first month
  NOAA_first_day    = wday(NOAA_dates[1])
  if (NOAA_first_day==1) {
    NOAA_first_sunday = NOAA_dates[1]
  } else {
    NOAA_first_sunday = NOAA_dates[1] + (8-NOAA_first_day)
  }
  if (mday(NOAA_dates[1])==1) {
    NOAA_first_month = NOAA_dates[1]
  } else {
    NOAA_first_month = NOAA_dates[1]
    month(NOAA_first_month) = month(NOAA_first_month) + 1
    mday(NOAA_first_month)  = 1
  }
  
  clim_by_disease = dbReadTable(myDB, name="clim_by_disease")
  
  cat("Begin aggregating daily climate data to inidence-cadence matching climate data. \n")
  keys = NOAA_keys
  
  luts =list()
  for(disease in diseases) {
    luts[[disease]] = dbReadTable(conn=myDB, name=paste0(disease,"_lut"))
  }
  
  
  # for each key, determine date range needed for each cadence
  cad_clim = list()
  for (key in keys) {
    cat("Spatial key ", key, " \n")
    cad_clim[[key]] = list()
    cad_clim[[key]]$cads = data.frame(cadence=integer(), min_date=as.Date(integer(), origin="1900-01-01"), max_date=as.Date(integer(), origin="1900-01-01"))
    for (disease in diseases) {
      # determine all master_keys that reference this 'key' for climate data
      key_ident = luts[[disease]]$identifier[luts[[disease]]$master_key==key]
      master_keys = luts[[disease]]$master_key[luts[[disease]]$clim_ident==key_ident]
      if (length(master_keys)>0) {
        # determine if key exists in disease data
        key_data = dbGetQuery(conn=myDB, statement=paste0("SELECT * FROM ", disease, "_data WHERE master_key IN ('", paste(master_keys, collapse="','"), "')"))
      } else {
        next
      }
      
      if (length(key_data$master_key)>0) {
        key_data$date = as.Date(key_data$date)
        cad_clim[[key]][[disease]] = key_data
        sources = unique(key_data$source_key)
        cadences = source_table$cadence[source_table$source_key %in% sources]
        cad_diff = setdiff(cadences, cad_clim[[key]]$cads$cadence)
        if (length(cad_diff)>0) {
          cad_clim[[key]]$cads = rbind(cad_clim[[key]]$cads, data.frame(cadence=cad_diff, min_date=as.Date(NA), max_date=as.Date(NA)))
        }
        
        for (ii in 1:length(cadences)) {
          cad_ind = key_data$source_key==sources[ii]
          cad_ind2 = cad_clim[[key]]$cads$cadence==cadences[ii]
          cad_clim[[key]]$cads$min_date[cad_ind2] = min(cad_clim[[key]]$cads$min_date[cad_ind2], key_data$date[cad_ind], na.rm=T)
          cad_clim[[key]]$cads$max_date[cad_ind2] = max(cad_clim[[key]]$cads$max_date[cad_ind2], key_data$date[cad_ind], na.rm=T)
        }
        
        # Special rules for master_keys that are in the LUT, but have no incidence
      } else if (disease=="flu") {
        # if flu, but no incidence data is available, add a weekly climate requirement
        # this rule is primarily to ensure we get climate data for Florida
        if (!(2 %in% cad_clim[[key]]$cads$cadence)) {
          cad_clim[[key]]$cads = rbind(cad_clim[[key]]$cads, data.frame(cadence=2, min_date=as.Date(NA), max_date=as.Date(NA)))
        }
      } else {
        next
      }
    }
    
    if (nrow(cad_clim[[key]]$cads)==0) {
      # skip to next key
      next
    }
    
    # determine last day of daily data
    daily_max = dbGetQuery(myDB, statement=paste0("SELECT MAX(date) FROM noaa_daily WHERE master_key='", key, "';"))
    if (nrow(daily_max)>0) {
      daily_max = as.Date(daily_max$max)
      # pull all daily climate data for this key.  At this point, it would probably be faster to pull only the days needed for each cadence-update
      temp_clim = dbGetQuery(myDB, statement=paste0("SELECT * FROM noaa_daily WHERE master_key='", key, "' ORDER BY date;"))
      temp_clim$date = as.Date(temp_clim$date)
      for (ii in 1:length(cad_clim[[key]]$cads$cadence)) {
        # average daily to appropriate cadence
        if (cad_clim[[key]]$cads$cadence[ii]==1) {
          # daily, do nothing
        } else if (cad_clim[[key]]$cads$cadence[ii]==2) {
          # first check if new weeks can be calced
          # find latest noaa_weekly date
          weekly_max = dbGetQuery(myDB, statement=paste0("SELECT MAX(date) FROM noaa_weekly WHERE master_key='", key, "';"))
          weekly_max = as.Date(weekly_max[[1]])
          
          if (is.na(weekly_max)) {
            # calculate for all daily dates
            temp_ind = temp_clim$date>=NOAA_first_sunday
            nweeks = sum(temp_ind) %/% 7
            date = seq(from=NOAA_first_sunday, by=7, length.out=nweeks)
          } else if (weekly_max+13 <= daily_max) {
            # calculate update only
            temp_ind = temp_clim$date>=(weekly_max+7)
            nweeks = sum(temp_ind) %/% 7
            date = weekly_max + 7*(1:nweeks)
          } else {
            # no update needed
            next
          }
          # average to weekly
          avg_clim = data.frame(master_key=key, date=date)
          avg_clim$sh = .colMeans(temp_clim$sh[temp_ind], m=7, n=nweeks, na.rm=TRUE)
          avg_clim$temp = .colMeans(temp_clim$temp[temp_ind], m=7, n=nweeks, na.rm=TRUE)
          avg_clim$precip = .colMeans(temp_clim$precip[temp_ind], m=7, n=nweeks, na.rm=TRUE)
          # write to database
          dbWriteTable(myDB, name="noaa_weekly", value=avg_clim, append=TRUE, overwrite=FALSE, row.names=FALSE)
        } else if (cad_clim[[key]]$cads$cadence[ii]==3) {
          # first check if new months can be calced
          # find latest noaa_monthly date
          monthly_max = dbGetQuery(myDB, statement=paste0("SELECT MAX(date) FROM noaa_monthly WHERE master_key='", key, "';"))
          monthly_max = as.Date(monthly_max[[1]])
          
          if (is.na(monthly_max)) {
            # process all NOAA dates
            YMindex = data.frame(year=year(temp_clim$date), month=month(temp_clim$date))
            # create a sequence of months
            years = year(NOAA_first_month):year(temp_clim$date[nrow(temp_clim)])
            months = rep(1:12, length(years))
            years = rep(years, each=12)
            remove_ind = years==year(NOAA_first_month) & months<month(NOAA_first_month)
            remove_ind = remove_ind | (years==year(temp_clim$date[nrow(temp_clim)]) & months>month(temp_clim$date[nrow(temp_clim)]))
            months = months[!remove_ind]
            years  = years[!remove_ind]
          } else if ((month(monthly_max)<month(daily_max) && year(monthly_max)==year(daily_max)) || year(monthly_max)<year(daily_max)) {
            YMindex = data.frame(year=year(temp_clim$date), month=month(temp_clim$date))
            # create a sequence of months
            years = year(monthly_max):year(daily_max)
            months = rep(1:12, length(years))
            years = rep(years, each=12)
            remove_ind = years==year(monthly_max) & months<=month(monthly_max)
            remove_ind = remove_ind | (years==year(temp_clim$date[nrow(temp_clim)]) & months>month(temp_clim$date[nrow(temp_clim)]))
            months = months[!remove_ind]
            years  = years[!remove_ind]
          } else {
            # no update needed
            next
          }
          # average to monthly
          avg_clim = data.frame(master_key=key, date=as.Date(paste0(years,"-",months,"-01")))
          
          for (jj in 1:nrow(avg_clim)) {
            month_ind = YMindex$month==month(avg_clim$date[jj]) & YMindex$year==year(avg_clim$date[jj])
            avg_clim$sh[jj] = mean(temp_clim$sh[month_ind], na.rm=TRUE)
            avg_clim$temp[jj] = mean(temp_clim$temp[month_ind], na.rm=TRUE)
            avg_clim$precip[jj] = mean(temp_clim$precip[month_ind], na.rm=TRUE)
          }
          # write to database
          dbWriteTable(myDB, name="noaa_monthly", value=avg_clim, append=TRUE, overwrite=FALSE, row.names=FALSE)
          
        } else if (cad_clim[[key]]$cads$cadence[ii]==4) {
          # check if new years can be calced
          # find latest noaa_yearly date
          yearly_max = dbGetQuery(myDB, statement=paste0("SELECT MAX(date) FROM noaa_yearly WHERE master_key='", key, "';"))
          yearly_max = as.Date(yearly_max[[1]])
          
          # average to yearly
          if (is.na(yearly_max)) {
            # process all years
            Yindex = year(temp_clim$date)
            years = unique(Yindex)
            # years = year(temp_clim$date[1]):year(temp_clim$date[nrow(temp_clim)])
          } else if (year(daily_max)>=year(yearly_max)) {
            # calc update years. this will most often result in an update of the current (incomplete) year.
            Yindex = year(temp_clim$date)
            years = year(daily_max):year(yearly_max)
          } else {
            # no update needed
            next
          }
          
          avg_clim = data.frame(master_key=key, date=as.Date(paste0(years, "-01-01")))
          for (jj in 1:length(years)) {
            year_ind = Yindex==years[jj]
            
            avg_clim$sh[jj] = mean(temp_clim$sh[year_ind], na.rm=TRUE)
            avg_clim$temp[jj] = mean(temp_clim$temp[year_ind], na.rm=TRUE)
            avg_clim$precip[jj] = mean(temp_clim$precip[year_ind], na.rm=TRUE)
          }
          # first clear any overlapping rows
          dbExecute(myDB, statement=paste0("DELETE FROM noaa_yearly WHERE master_key='", key, "' AND date IN('", paste(avg_clim$date, collapse="','"), "');"))
          # write to database
          dbWriteTable(myDB, name="noaa_yearly", value=avg_clim, append=TRUE, overwrite=FALSE, row.names=FALSE)
        } 
      } # end cadence loop
    } else {
      cat("This master_key has no daily data. Skipping.\n\n")
    }
    
    
  }
  
  dbDisconnect(myDB)
}


# subroutine of NOAAadm_area_idents.  This function allows the use of apply() across the temporal dimension when mapping NOAA grid to population-weighted master_keys
AverageRows = function(data=NULL, weights=NULL) {
  average = sum(data*weights, na.rm=TRUE)
}

# Open a connection to BSVE PostGreSQL database.  Be sure to dbDisconnect() when done.
OpenCon <- function(sql_db='cbip') {
  #' Open a Connection to MySQL/PostGreSQL Database.
  #'
  #' This function uses guest credectials to open a read-only connection to the MySQL mydatabase.
  #' @param sql_db Either 'PredSci' or 'BSVE'
  #' @return myDB  A S4 object that inherits from DBIConnection.
  #' @examples
  #' require(DICE)
  #' myDB = OpenCon(sql_db='PredSci')
  
  if (tolower(sql_db)=="predsci") {
    drv  = MySQL()
    user = "cbip_read"
    password="Bb*Ub#704#Dz3s"
    dbname='epi_data'
    port = 3306
    if (Sys.info()["nodename"]=="Q") {
      host="shadow"
    } else {
      host="shadow.predsci.com"
    }
  } else if (tolower(sql_db)=="bsve") {
    drv = dbDriver("PostgreSQL")
    user="bsve_at_20499"
    port=5432
    password="vHFUYR52"
    host="dataservices-postgresql.bsvecosystem.net"
    dbname="displaydicedata"
  } else if (tolower(sql_db)=="cbip") {
    drv = MySQL()
    user=""
    port=5432
    password=""
    host=""
    dbname="displaydicedata"
  } else if (tolower(sql_db)=="dcs_aws") {
    drv = MySQL()
    user = "dice_sync"
    host = "cbip.cxg97l2hxl8x.us-east-1.rds.amazonaws.com"
    password = "dice123$$"
    port = 3306
    dbname="epi_data"
  }
  
  for (ii in 1:10) {
    # attempt to establish the database connection
    myDB=try(dbConnect(drv=drv, user=user, password=password, dbname=dbname, host=host, port=port), silent=TRUE)
    if (!is(myDB, "try-error")) {
      # if successful, continue
      break
    } else {
      cat("Failed to connect to ", sql_db, " database on attempt ", ii, ".\n", sep="")
      # if not, wait 0.1s and try again
      Sys.sleep(0.1)
    }
  }
  
  # if loop exited without making a connection, throw error
  if (is(myDB, "try-error")) {
    stop("After ", ii, " attempts, could not establish a connection to ", sql_db, " database.\n")
  }
  
  return(myDB)
}
