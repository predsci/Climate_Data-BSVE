# This R script will load the R library packages required for the displayDice service.
# To run enter 'Rscript installRpackages.R'

HOME_DIR<-"/home/shiny"

rm(list = ls())
options(repos = "https://cran.rstudio.com/")

libdir <- "/usr/local/lib/R/site-library"
.libPaths(libdir)
unlink (file.path(libdir, "LOCK00*"), recursive = TRUE)

# if (!require(shiny)) {
# 	  install.packages("shiny")
# }
# if (!require(plotly)) {
#   install.packages("plotly")
# }
if (! require (raster)) {
  install.packages("raster")
  Sys.sleep(1)
}
if (! require (DBI)) {
  install.packages("DBI")
}
if (! require (dplyr)) {
  install.packages("dplyr")
}

if (! require (RMySQL)) {
  install.packages("RMySQL")
}
if (! require (RPostgreSQL)) {
  install.packages("RPostgreSQL")
}
if (! require (countrycode)) {
  install.packages("countrycode")
}
if (! require (stringr)) {
  install.packages("stringr")
}
if (! require (curl)) {
  install.packages("curl")
}
if (! require (ncdf4)) {
  install.packages("ncdf4")
}
if (! require (lubridate)) {
  install.packages("lubridate")
}

Sys.sleep(1)
