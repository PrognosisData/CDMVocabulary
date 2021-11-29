# executes code to load/reload vocabulary tables
#
# assumptions:
#   latest

if (!require("devtools")) install.packages("devtools")
devtools::install_github("OHDSI/CommonDataModel", "v5.4")
library(CommonDataModel)
library(DatabaseConnector)
library(CDMVocabulary)
install.packages("tictoc")
library(tictoc)
install.packages("readr")
library(readr)
install.packages("dplyr")
library(dplyr)
install.packages("stringr")
library(stringr)
Sys.setenv("DATABASECONNECTOR_JAR_FOLDER" = "./jdbc")
downloadJdbcDrivers("sql server")
downloadJdbcDrivers("postgresql")

connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "dbms",
                                            server = "localhost",
                                            user = "user",
                                            password = "password")

dropConstraints(connectionDetails, "5.4", "omop540")
dropTables(connectionDetails, "5.4", "omop540")

createTables(connectionDetails, "5.4", "omop540")
dropConstraints(connectionDetails, "5.4", "omop540")
truncateVocabTables(connectionDetails, "5.4", "omop540")
loadVocabTables(connectionDetails, "5.4", "omop540", "vocabulary")
reapplyConstraints(connectionDetails, "5.4", "omop540")


