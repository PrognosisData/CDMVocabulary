# @file loadVocabulary.R
#
# Copyright 2019 Observational Health Data Sciences and Informatics
# Portions Copyright 2021 Prognosis Data Corp
#
# This file is part of CDMVocabulary
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#' @title Creates OMOP CDM tables in database.
#' @description Creates OMOP CDM tables in database
#' @param connectionDetails An object of class connectionDetails as created by the DatabaseConnector::createConnectionDetails function.
#' @param cdmVersion The version of the CDM you are creating, e.g. 5.3, 5.4
#' @param cdmDatabaseSchema The schema of the CDM instance where the DDL will be run. For example, this would be "ohdsi.dbo" when testing on sql server.
#' @param ... Other arguments passed on to DatabaseConnector::executeSql. (This allows the user to set the path to errorReportFile.)
#' @export
#'
createCDMTables <- function(connectionDetails,
                         cdmVersion,
                         cdmDatabaseSchema,
                         ...) {

  outputfolder <- tempdir(check = TRUE)

  filename <- outputCreateTables(targetDialect = connectionDetails$dbms,
                                cdmVersion = cdmVersion,
                                cdmDatabaseSchema = cdmDatabaseSchema,
                                outputfolder = outputfolder)
  sql <- paste(readr::read_file(file.path(outputfolder, filename)), sep = "\n")

  con <- DatabaseConnector::connect(connectionDetails = connectionDetails)

  DatabaseConnector::executeSql(con, sql = sql, ...)

  DatabaseConnector::disconnect(con)

}

#' @title Drops OMOP CDM constraints
#' @description Drops OMOP CDM constraints (foreign keys, primary keys, and indexes)
#' @export
dropConstraints <- function(connectionDetails,
                            cdmVersion,
                            cdmDatabaseSchema,
                            ...) {

  outputfolder <- tempdir(check = TRUE)

  filename <- outputDropIndexes(targetDialect = connectionDetails$dbms,
                                   cdmVersion = cdmVersion,
                                   cdmDatabaseSchema = cdmDatabaseSchema,
                                   outputfolder = outputfolder)
  sql <- paste(readr::read_file(file.path(outputfolder, filename)), sep = "\n")

  filename <- outputDropForeignKeys(targetDialect = connectionDetails$dbms,
                                   cdmVersion = cdmVersion,
                                   cdmDatabaseSchema = cdmDatabaseSchema,
                                   outputfolder = outputfolder)
  sql <- paste(sql, readr::read_file(file.path(outputfolder, filename)), sep = "\n")

  filename <- outputDropPrimaryKeys(targetDialect = connectionDetails$dbms,
                                   cdmVersion = cdmVersion,
                                   cdmDatabaseSchema = cdmDatabaseSchema,
                                   outputfolder = outputfolder)
  sql <- paste(sql, readr::read_file(file.path(outputfolder, filename)), sep = "\n")

  con <- DatabaseConnector::connect(connectionDetails = connectionDetails)

  DatabaseConnector::executeSql(con, sql = sql, ...)

  DatabaseConnector::disconnect(con)
}

#' @title Truncates OMOP CDM vocabulary tables
#' @describeIn Truncates OMOP CDM vocabulary tables
#' @export
truncateVocabTables <- function(connectionDetails,
                                cdmVersion,
                                cdmDatabaseSchema,
                                ...) {

  outputfolder <- tempdir(check = TRUE)

  filename <- outputTruncateVocab(targetDialect = connectionDetails$dbms,
                                  cdmVersion = cdmVersion,
                                  cdmDatabaseSchema = cdmDatabaseSchema,
                                  outputfolder = outputfolder)

  sql <- paste(readr::read_file(file.path(outputfolder, filename)), sep = "\n")

  con <- DatabaseConnector::connect(connectionDetails = connectionDetails)

  DatabaseConnector::executeSql(con, sql = sql, ...)

  DatabaseConnector::disconnect(con)
}

#' @title Loads the OMOP CDM vocabulary tables from csv files
#' @description Loads the OMOP CDM vocabulary tables from csv files
#' @param connectionDetails  An R object of type\cr\code{connectionDetails} created using the
#'                                     function \code{createConnectionDetails} in the
#'                                     \code{DatabaseConnector} package.
#' @param cdmSchema  The name of the database schema that will contain the Vocabulary (and CDM)
#'                                     tables.  Requires read and write permissions to this database. On SQL
#'                                     Server, this should specifiy both the database and the schema,
#'                                     so for example 'cdm_instance.dbo'.
#' @param vocabFileLoc     The location of the vocabulary csv files.
#' @param bulkLoad       Boolean flag indicating whether or not to use bulk loading (if possible).  Default is FALSE.
#' @export
#'
loadVocabTables <- function (connectionDetails, cdmSchema, vocabFileLoc, bulkLoad = FALSE)
{

  csvList <- c("concept.csv","vocabulary.csv","concept_ancestor.csv","concept_relationship.csv","relationship.csv","concept_synonym.csv","domain.csv","concept_class.csv", "drug_strength.csv")

  fileList <- list.files(vocabFileLoc)

  fileList <- fileList[which(tolower(fileList) %in% csvList)]

  conn <- DatabaseConnector::connect(connectionDetails)

  for (csv in fileList) {

    vocabTable <- data.table::fread(file = paste0(vocabFileLoc, "/", csv), stringsAsFactors = FALSE, header = TRUE, sep = "\t", na.strings = "")
    vocabTable <- as.data.frame(vocabTable)

    # Format Dates for tables that need it
    if (tolower(csv) == "concept.csv" || tolower(csv) == "concept_relationship.csv" || tolower(csv) == "drug_strength.csv") {

      vocabTable$valid_start_date <- as.Date(as.character(vocabTable$valid_start_date),"%Y%m%d")
      vocabTable$valid_end_date   <- as.Date(as.character(vocabTable$valid_end_date),"%Y%m%d")
    }

    writeLines(paste0("Loading: ",csv))

    suppressWarnings({
      DatabaseConnector::insertTable(conn,tableName=paste0(cdmSchema,".",strsplit(csv,"[.]")[[1]][1]), data=as.data.frame(vocabTable), dropTableIfExists = FALSE, createTable = FALSE, useMppBulkLoad = bulkLoad, progressBar = TRUE)
    })
  }

  on.exit(DatabaseConnector::disconnect(conn))
}

insertVocabData <- function(conn, tableName) {
  function(vocabData, pos) {
    vocabTable <- as.data.frame(vocabData)

    # Format Dates for tables that need it
    if (tolower(tableName) == "concept" || tolower(tableName) == "concept_relationship" || tolower(tableName) == "drug_strength") {
      vocabTable$valid_start_date <- as.Date(as.character(vocabTable$valid_start_date),"%Y%m%d")
      vocabTable$valid_end_date   <- as.Date(as.character(vocabTable$valid_end_date),"%Y%m%d")
    }

    # suppressWarnings({
    #   DatabaseConnector::insertTable(conn, tableName=tableName, data=as.data.frame(vocabTable), dropTableIfExists = FALSE, createTable = FALSE, useMppBulkLoad = FALSE, progressBar = FALSE)
    # })

    tryCatch(DatabaseConnector::insertTable(conn, tableName=tableName, data=as.data.frame(vocabTable), dropTableIfExists = FALSE, createTable = FALSE, useMppBulkLoad = FALSE, progressBar = FALSE),
             warning = function(w) { message(paste0(pos, ": ", w))},
             error = function(e) { message(paste0(pos, ": ", e))})
  }
}

getColumnTypes <- function(cdmVersion, tableName) {

  cdmFieldCsvLoc <- system.file(file.path("csv", paste0("OMOP_CDMv", cdmVersion, "_Field_Level.csv")), package = "CommonDataModel", mustWork = TRUE)
  cdmSpecs <- read.csv(cdmFieldCsvLoc, stringsAsFactors = FALSE)
  fields <- subset(cdmSpecs, cdmTableName == tableName)
  fieldTypes <- fields$cdmDatatype
  fieldNames <- fields$cdmFieldName

  columnTypes <- ""
  for (fieldType in fieldTypes) {
    if (fieldType == "varchar") {
      columnType <- "c"
    } else if (startsWith(fieldType, "varchar")[1]) {
      columnType <- "c"
    } else if (fieldType == "integer") {
      columnType <- "i"
    } else if (fieldType == "bigint") {
      columnType <- "i"
    } else if (startsWith(fieldType, "number")[1]) {
      columnType <- "n"
    } else if (fieldType == "date") {
      columnType <- "c"
    } else if (fieldType == "datetime") {
      columnType <- "c"
    } else if (fieldType == "float") {
      columnType <- "d"
    } else {
      columnType <- "?"
    }

    columnTypes <- paste0(columnTypes, columnType)
  }

  return(columnTypes)

}


#' @title Loads the OMOP CDM vocabulary tables from csv files
#' @description Loads the OMOP CDM vocabulary tables from csv files
#' @param connectionDetails  An R object of type\cr\code{connectionDetails} created using the
#'                                     function \code{createConnectionDetails} in the
#'                                     \code{DatabaseConnector} package.
#' @param cdmVersion The version of the CDM you are creating, e.g. 5.3, 5.4
#' @param cdmSchema  The name of the database schema that will contain the Vocabulary (and CDM)
#'                                     tables.  Requires read and write permissions to this database. On SQL
#'                                     Server, this should specifiy both the database and the schema,
#'                                     so for example 'cdm_instance.dbo'.
#' @param vocabFileLoc The location of the vocabulary csv files.
#' @param chuckSize Number of records in each chunck of data that is processed.  Default is 10000
#' @export
loadVocabTables2 <- function(connectionDetails, cdmVersion, cdmSchema, vocabFileLoc, chunkSize = 10000)
{

  csvList <- c("concept.csv","vocabulary.csv","concept_ancestor.csv","concept_relationship.csv","relationship.csv","concept_synonym.csv","domain.csv","concept_class.csv", "drug_strength.csv")

  fileList <- list.files(vocabFileLoc)
  fileList <- fileList[which(tolower(fileList) %in% csvList)]

  conn <- DatabaseConnector::connect(connectionDetails)

  for (csv in fileList) {
    writeLines(paste0("Loading: ", csv))
    tableName <- strsplit(csv,"[.]")[[1]][1]
    columnTypes <- getColumnTypes(cdmVersion, tableName)

    readr::read_tsv_chunked(paste0(vocabFileLoc, "/", csv),
                            readr::DataFrameCallback$new(insertVocabData(conn, paste0(cdmSchema, ".", tableName))),
                            col_types = columnTypes,
                            quote = "",
                            chunk_size = chunkSize)
    break
  }

  on.exit(DatabaseConnector::disconnect(conn))
}


#' @title Reapplies constraints on OMOP CDM vocabulary tables.
#' @describeIn Reapplies constraints on OMOP CDM vocabulary tables.
#' @export
reapplyConstraints <- function(connectionDetails,
                            cdmVersion,
                            cdmDatabaseSchema,
                            ...) {

  outputfolder <- tempdir(check = TRUE)

  filename <- outputCreatePrimaryKeys(targetDialect = connectionDetails$dbms,
                                cdmVersion = cdmVersion,
                                cdmDatabaseSchema = cdmDatabaseSchema,
                                outputfolder = outputfolder)
  sql <- paste(readr::read_file(file.path(outputfolder, filename)), sep = "\n")

  filename <- outputCreateForeignKeys(targetDialect = connectionDetails$dbms,
                                    cdmVersion = cdmVersion,
                                    cdmDatabaseSchema = cdmDatabaseSchema,
                                    outputfolder = outputfolder)
  sql <- paste(sql, readr::read_file(file.path(outputfolder, filename)), sep = "\n")

  filename <- outputCreateIndexes(targetDialect = connectionDetails$dbms,
                                    cdmVersion = cdmVersion,
                                    cdmDatabaseSchema = cdmDatabaseSchema,
                                    outputfolder = outputfolder)
  sql <- paste(sql, readr::read_file(file.path(outputfolder, filename)), sep = "\n")

  con <- DatabaseConnector::connect(connectionDetails = connectionDetails)

  DatabaseConnector::executeSql(con, sql = sql, ...)

  DatabaseConnector::disconnect(con)
}
