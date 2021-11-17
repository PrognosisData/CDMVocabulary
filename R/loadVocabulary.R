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

#' Generate and execute the DDL on a database
#'
#' This function will generate the DDL for a specific dbms and CDM version and
#' then execute the DDL on a database.
#'
#' @description dropConstraints Drops table constraints.
#' @param connectionDetails An object of class connectionDetails as created by the DatabaseConnector::createConnectionDetails function.
#' @param cdmVersion The version of the CDM you are creating, e.g. 5.3, 5.4
#' @param cdmDatabaseSchema The schema of the CDM instance where the DDL will be run. For example, this would be "ohdsi.dbo" when testing on sql server.
#' @param ... Other arguments passed on to DatabaseConnector::executeSql. (This allows the user to set the path to errorReportFile.)
#' @export
#'
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

#' @describeIn loadVocabulary truncateVocabTables Truncates vocabulary tables
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

#' Function from ETL-Synthea project
#'
#' @description This function populates all Vocabulary tables with data in csv files.
#'
#' @usage LoadVocabFromCsv(connectionDetails, cdmSchema, vocabFileLoc, bulkLoad)
#'
#' @details This function assumes \cr\code{createCDMTables()} has already been run.
#'
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

#' @describeIn loadVocabulary reapplyConstraints Reapplies table constraints.
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
