# Copyright 2019 Observational Health Data Sciences and Informatics
# Portions Copyright 2021 Prognosis Data Corp

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

#' outputDdl DDL script
#'
#' Write the drop DDL to a SQL file. The SQL will be rendered (parameters replaced) and translated to the target SQL
#' dialect. By default the @cdmDatabaseSchema parameter is kept in the SQL file and needs to be replaced before
#' execution.
#'
#' @param targetDialect  The dialect of the target database. Choices are "oracle", "postgresql", "pdw", "redshift", "impala", "netezza", "bigquery", "sql server"
#' @param cdmVersion The version of the CDM you are creating, e.g. 5.3, 5.4
#' @param outputfolder The directory or folder where the SQL file should be saved.
#' @param cdmDatabaseSchema The schema of the CDM instance where the DDL will be run. For example, this would be "ohdsi.dbo" when testing on sql server.
#'                          Defaults to "@cdmDatabaseSchema"
#'
#' @describeIn outputDdl outputCreateTables Write the SQL code that drops the primary keys to a file.
#' @export
outputCreateTables <- function(targetDialect, cdmVersion, outputfolder, cdmDatabaseSchema = "@cdmDatabaseSchema") {

  stopifnot(cdmVersion %in% listSupportedVersions())
  sql <- formatCreateTables(cdmVersion)
  filename <- paste("OMOPCDM", gsub(" ", "_", targetDialect), cdmVersion, "create", "tables.sql", sep = "_")
  outputDdl(sql, filename, targetDialect, cdmVersion, outputfolder, cdmDatabaseSchema)

}


#' @describeIn outputDdl outputCreatePrimaryKeys Write the SQL code that drops the primary keys to a file.
#' @export
outputCreatePrimaryKeys <- function(targetDialect, cdmVersion, outputfolder, cdmDatabaseSchema = "@cdmDatabaseSchema") {

  stopifnot(cdmVersion %in% listSupportedVersions())
  sql <- formatCreatePrimaryKeys(cdmVersion)
  filename <- paste("OMOPCDM", gsub(" ", "_", targetDialect), cdmVersion, "create", "primary", "keys.sql", sep = "_")
  outputDdl(sql, filename, targetDialect, cdmVersion, outputfolder, cdmDatabaseSchema)

}

#' @describeIn outputDdl outputCreatetForeignKeys Write the SQL code that drops the primary keys to a file.
#' @export
outputCreateForeignKeys <- function(targetDialect, cdmVersion, outputfolder, cdmDatabaseSchema = "@cdmDatabaseSchema") {

  stopifnot(cdmVersion %in% listSupportedVersions())
  sql <- formatCreateForeignKeys(cdmVersion)
  filename <- paste("OMOPCDM", gsub(" ", "_", targetDialect), cdmVersion, "create", "foreign", "keys.sql", sep = "_")
  outputDdl(sql, filename, targetDialect, cdmVersion, outputfolder, cdmDatabaseSchema)

}

#' @describeIn outputDdl outputCreateIndexes Write the SQL code that creates indexes to a file.
#' @export
outputCreateIndexes <- function(targetDialect, cdmVersion, outputfolder, cdmDatabaseSchema = "@cdmDatabaseSchema") {

  stopifnot(cdmVersion %in% listSupportedVersions())
  sql <- formatCreateIndexes(cdmVersion)
  filename <- paste("OMOPCDM", gsub(" ", "_", targetDialect), cdmVersion, "create", "indices.sql", sep = "_")
  outputDdl(sql, filename, targetDialect, cdmVersion, outputfolder, cdmDatabaseSchema)

}


#' @describeIn outputDdl outputDropPrimaryKeys Write the SQL code that drops the primary keys to a file.
#' @export
outputDropTables <- function(targetDialect, cdmVersion, outputfolder, cdmDatabaseSchema = "@cdmDatabaseSchema") {

  stopifnot(cdmVersion %in% listSupportedVersions())
  sql <- formatDropTables(cdmVersion)
  filename <- paste("OMOPCDM", gsub(" ", "_", targetDialect), cdmVersion, "drop", "tables.sql", sep = "_")
  outputDdl(sql, filename, targetDialect, cdmVersion, outputfolder, cdmDatabaseSchema)

}


#' @describeIn outputDdl outputDropPrimaryKeys Write the SQL code that drops the primary keys to a file.
#' @export
outputDropPrimaryKeys <- function(targetDialect, cdmVersion, outputfolder, cdmDatabaseSchema = "@cdmDatabaseSchema") {

  stopifnot(cdmVersion %in% listSupportedVersions())
  sql <- formatDropPrimaryKeys(cdmVersion)
  filename <- paste("OMOPCDM", gsub(" ", "_", targetDialect), cdmVersion, "drop", "primary", "keys.sql", sep = "_")
  outputDdl(sql, filename, targetDialect, cdmVersion, outputfolder, cdmDatabaseSchema)

}


#' @describeIn outputDdl outputDropForeignKeys Write the SQL code that drops the foreign keys to a file.
#' @export
outputDropForeignKeys <- function(targetDialect, cdmVersion, outputfolder, cdmDatabaseSchema = "@cdmDatabaseSchema") {

  stopifnot(cdmVersion %in% listSupportedVersions())
  sql <- formatDropForeignKeys(cdmVersion)
  filename <- paste("OMOPCDM", gsub(" ", "_", targetDialect), cdmVersion, "drop", "foreign", "keys.sql", sep = "_")
  outputDdl(sql, filename, targetDialect, cdmVersion, outputfolder, cdmDatabaseSchema)

}


#' @describeIn outputDdl outputDropIndices Write SQL code that drops the indexes to a file.
#' @export
outputDropIndexes <- function(targetDialect, cdmVersion, outputfolder, cdmDatabaseSchema  = "@cdmDatabaseSchema") {

  stopifnot(cdmVersion %in% listSupportedVersions())
  sql <- formatDropIndexes(cdmVersion)

  # Remove "on <table>" from sql for postgresql dialect
  if (targetDialect == "postgresql") {
    sql <- str_replace_all(sql, " ON [@a-zA-Z0-9._]+;", ";")
    sql <- str_replace_all(sql, "IF EXISTS ", "IF EXISTS @cdmDatabaseSchema.")
  }

  filename <- paste("OMOPCDM", gsub(" ", "_", targetDialect), cdmVersion, "drop", "indexes.sql", sep = "_")
  outputDdl(sql, filename, targetDialect, cdmVersion, outputfolder, cdmDatabaseSchema)

}


#' @describeIn outputDdl outputTruncateVocab Write SQL code that truncates the vocabulary tables to a file.
#' @export
outputTruncateVocab <- function(targetDialect, cdmVersion, outputfolder, cdmDatabaseSchema  = "@cdmDatabaseSchema") {

  stopifnot(cdmVersion %in% listSupportedVersions())
  sql <- formatTruncateVocab(cdmVersion)
  filename <- paste("OMOPCDM", gsub(" ", "_", targetDialect), cdmVersion, "trunate", "vocab.sql", sep = "_")
  outputDdl(sql, filename, targetDialect, cdmVersion, outputfolder, cdmDatabaseSchema)

}

#' @describeIn outputDdl outputDdl Write SQL code that drops the indexes to a file.
outputDdl <- function(sql, filename, targetDialect, cdmVersion, outputfolder, cdmDatabaseSchema) {

  # argument checks
  stopifnot(targetDialect %in% c("oracle", "postgresql", "pdw", "redshift", "impala", "netezza", "bigquery", "sql server"))
  stopifnot(cdmVersion %in% listSupportedVersions())
  stopifnot(is.character(cdmDatabaseSchema))

  if(missing(outputfolder)) {
    outputfolder <- file.path("ddl", cdmVersion, gsub(" ", "_", targetDialect))
  }

  if(!dir.exists(outputfolder)) dir.create(outputfolder, showWarnings = FALSE, recursive = TRUE)

  sql <- SqlRender::render(sql = sql, cdmDatabaseSchema = cdmDatabaseSchema, targetDialect = targetDialect)
  sql <- SqlRender::translate(sql, targetDialect = targetDialect)

  SqlRender::writeSql(sql = sql, targetFile = file.path(outputfolder, filename))
  invisible(filename)
}
