# executes code to load/reload vocabulary tables
#
# assumptions:
#   latest

if (!require("devtools")) install.packages("devtools")
devtools::install_github("OHDSI/CommonDataModel", "v5.4")
library(CommonDataModel)
library(DatabaseConnector)
library(CDMVocabulary)

Sys.setenv("DATABASECONNECTOR_JAR_FOLDER" = ".\\jdbc")
downloadJdbcDrivers("sql server")


connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "sql server",
                                            server = "localhost",
                                            user = "pie",
                                            password = "demouser")

CommonDataModel::writeDdl("sql server", "5.4", "omop540")
CommonDataModel::writePrimaryKeys("sql server", "5.4", "omop540")
# CommonDataModel::writeForeignKeys("sql server", "5.4", "omop540")
CommonDataModel::writeIndex("sql server", "5.4", "omop540")
CommonDataModel::executeDdl(connectionDetails, "5.4", "omop540.dbo")

outputCreateTables("sql server", "5.4", "omop540")
outputCreatePrimaryKeys("sql server", "5.4", "omop540")
outputCreateForeignKeys("sql server", "5.4", "omop540")
outputCreateIndexes("sql server", "5.4", "omop540")
outputDropTables("sql server", "5.4", "omop540")
outputDropPrimaryKeys("sql server", "5.4", "omop540")
outputDropForeignKeys("sql server", "5.4", "omop540")
outputDropIndexes("sql server", "5.4", "omop540")

createCDMTables(connectionDetails, "5.4", "omop540.dbo")
dropConstraints(connectionDetails, "5.4", "omop540.dbo")
truncateVocabTables(connectionDetails, "5.4", "omop540.dbo")
loadVocabTables2(connectionDetails, "5.4", "omop540.dbo", "Z:\\Share\\vocabulary")
reapplyConstraints(connectionDetails, "5.4", "omop540.dbo")


install.packages("readr")
library(readr)
library(CommonDataModel)
library(DatabaseConnector)
library(CDMVocabulary)


con <- file("Z:\\Share\\vocabulary\\DOMAIN.csv", "r")

data <- read_tsv_chunked(con, nrows = 10)
data

close(con)


connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "sql server",
                                                                server = "localhost",
                                                                user = "pie",
                                                                password = "demouser")
f <- function(conn, cdmSchema, cdmTableName) {
  function(x, pos) {
    vocabTable <- as.data.frame(x)
    suppressWarnings({
      DatabaseConnector::insertTable(conn, tableName=paste0(cdmSchema, "." , cdmTableName), data=as.data.frame(vocabTable), dropTableIfExists = FALSE, createTable = FALSE, useMppBulkLoad = FALSE, progressBar = FALSE)
    })
  }
}
conn <- DatabaseConnector::connect(connectionDetails)
readr::read_tsv_chunked("Z:\\Share\\vocabulary\\DOMAIN.csv",
                        readr::DataFrameCallback$new(f(conn, "omop540.dbo", "DOMAIN")),
                        chunk_size = 10)
DatabaseConnector::disconnect(conn)



read_tsv_chunked("Z:\\Share\\vocabulary\\DOMAIN.csv", DataFrameCallback$new(function(x, pos) {
  message(paste(pos / 10, x, ","))
}), chunk_size = 10)


startsWith("varchar(20)", "varchar")


