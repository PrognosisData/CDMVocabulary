# Copyright 2019 Observational Health Data Sciences and Informatics
# Portions Copyright 2021 Prognosis Data Corp
#
# This file is part of CDMVocabulary package.
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

#' Create scripts to drop the OMOP CDM objects.
#'
#' The formatDropTables, formatDropForeignKeys, and formatDropPrimaryKeys functions each return a character string
#' containing their respective DDL SQL code in OHDSQL dialect for a specific CDM version.
#' The SQL they generate needs to be rendered and translated before it can be executed.
#'
#' The DDL SQL code is created from a two csv files that detail the OMOP CDM Specifications.
#' These files also form the basis of the CDM documentation and the Data Quality
#' Dashboard.
#'
#' @describeIn formatDdl formatCreateTables Returns a string containing the OHDSQL for creation of tables in the OMOP CDM.
#' @param cdmVersion The version of the CDM you are creating, e.g. 5.3, 5.4
#' @return A character string containing the OHDSQL DDL
#' @importFrom utils read.csv
#' @export
#'
formatCreateTables <- function(cdmVersion) {
  # Use existing function from CommonDataModel package
  sql_result <- CommonDataModel::createDdl(cdmVersion)

  # Fix bug in CommonDataModel v5.4
  if (cdmVersion == "5.4") {
    sql_result <- gsub("person_id bigint NOT NULL",
                       "person_id integer NOT NULL",
                       sql_result,
                       fixed = TRUE)
  }

  return(paste0(sql_result, collapse = ""))
}

#' @describeIn formatDdl v Returns a string containing the OHDSQL for creation of primary keys in the OMOP CDM.
#' @export
formatCreatePrimaryKeys <- function(cdmVersion) {
  # Use existing function from CommonDataModel package
  sql_result = CommonDataModel::createPrimaryKeys(cdmVersion)

  # append pk fo cohort_definition to fix bug in CommonDataModel code for v5.4
  if (cdmVersion == "5.4") {
    sql_result <- paste0(sql_result, "\nALTER TABLE @cdmDatabaseSchema.COHORT_DEFINITION ADD CONSTRAINT xpk_COHORT_DEFINITION PRIMARY KEY NONCLUSTERED (cohort_definition_id);\n")
  }

  return(paste0(sql_result, collapse = ""))
}



#' @describeIn formatDdl formatCreateForeignKeys Returns a string containing the OHDSQL for creation of foreign keys in the OMOP CDM.
#' @export
formatCreateForeignKeys <- function(cdmVersion) {
  # Use existing function from CommonDataModel package
  sql_result <- CommonDataModel::createForeignKeys(cdmVersion)

  # Fix bug in CommonDataModel v5.4
  if (cdmVersion == "5.4") {
    sql_result <- gsub("ALTER TABLE @cdmDatabaseSchema.COHORT_DEFINITION ADD CONSTRAINT fpk_COHORT_DEFINITION_cohort_definition_id FOREIGN KEY (cohort_definition_id) REFERENCES @cdmDatabaseSchema.COHORT (COHORT_DEFINITION_ID)",
                       "ALTER TABLE @cdmDatabaseSchema.COHORT ADD CONSTRAINT fpk_COHORT_cohort_definition_id FOREIGN KEY (cohort_definition_id) REFERENCES @cdmDatabaseSchema.COHORT_DEFINITION (COHORT_DEFINITION_ID)",
                       sql_result,
                       fixed = TRUE)
  }

  return(paste0(sql_result, collapse = ""))
}

#' @describeIn formatDdl formatCreateIndexes Returns a string containing the OHDSQL for creation of indexes in the OMOP CDM.
#' @export
formatCreateIndexes <- function(cdmVersion) {
  # argument checks
  stopifnot(is.character(cdmVersion), length(cdmVersion) == 1, cdmVersion %in% listSupportedVersions())

  cdmIndexCsvLoc <- system.file(file.path("csv", paste0("OMOP_CDMv", cdmVersion, "_Indexes.csv")), package = "CDMVocabulary", mustWork = TRUE)
  cdmIndexSpecs <- read.csv(cdmIndexCsvLoc, stringsAsFactors = FALSE)

  sql_result <- c(paste0("--@targetDialect CDM Indexes for OMOP Common Data Model ", cdmVersion, "\n"))
  cdmTableName <- ""
  for (rowNum in 1:nrow(cdmIndexSpecs)) {

    cdmIndexRow <- cdmIndexSpecs[rowNum,]

    # If new table, then add newline between create index statements
    if (cdmTableName != cdmIndexRow$cdmTableName) {
      sql_result <- c(sql_result, paste0("\n"))
      cdmTableName <- cdmIndexRow$cdmTableName
    }

    sql_result <- c(sql_result, paste0("CREATE ", ifelse(cdmIndexRow$isClustered == "Yes", "CLUSTERED ", ""), "INDEX ", cdmIndexRow$indexName, " ON @cdmDatabaseSchema.", tolower(cdmIndexRow$cdmTableName), " (", cdmIndexRow$cdmFieldName, " ASC);\n"))

  }

  return(paste0(sql_result, collapse = ""))
}

#' @describeIn formatDdl formatDropTables Returns a string containing the OHDSQL for deletion of primary keys in the OMOP CDM.
#' @export
formatDropTables <- function(cdmVersion){

  # argument checks
  stopifnot(is.character(cdmVersion), length(cdmVersion) == 1, cdmVersion %in% listSupportedVersions())

  cdmTableCsvLoc <- system.file(file.path("csv", paste0("OMOP_CDMv", cdmVersion, "_Table_Level.csv")), package = "CommonDataModel", mustWork = TRUE)
  cdmFieldCsvLoc <- system.file(file.path("csv", paste0("OMOP_CDMv", cdmVersion, "_Field_Level.csv")), package = "CommonDataModel", mustWork = TRUE)

  tableSpecs <- read.csv(cdmTableCsvLoc, stringsAsFactors = FALSE)

  tableList <- tableSpecs$cdmTableName

  sql_result <- c()
  sql_result <- c(paste0("--@targetDialect CDM DDL Specification for OMOP Common Data Model ", cdmVersion, "\n"))
  for (tableName in tableList){
    sql_result <- c(sql_result, paste0("DROP TABLE IF EXISTS @cdmDatabaseSchema.", tableName, ";\n"))
  }
  return(paste0(sql_result, collapse = ""))
}


#' @describeIn formatDdl formatDropPrimaryKeys Returns a string containing the OHDSQL for deletion of primary keys in the OMOP CDM.
#' @export
formatDropPrimaryKeys <- function(cdmVersion){

  # argument checks
  stopifnot(is.character(cdmVersion), length(cdmVersion) == 1, cdmVersion %in% listSupportedVersions())

  cdmFieldCsvLoc <- system.file(file.path("csv", paste0("OMOP_CDMv", cdmVersion, "_Field_Level.csv")), package = "CommonDataModel", mustWork = TRUE)
  cdmSpecs <- read.csv(cdmFieldCsvLoc, stringsAsFactors = FALSE)

  primaryKeys <- subset(cdmSpecs, isPrimaryKey == "Yes")
  pkFields <- primaryKeys$cdmFieldName

  sql_result <- c(paste0("--@targetDialect CDM Primary Key Constraints for OMOP Common Data Model ", cdmVersion, "\n"))
  for (pkField in pkFields){

    subquery <- subset(primaryKeys, cdmFieldName==pkField)

    sql_result <- c(sql_result, paste0("\nALTER TABLE @cdmDatabaseSchema.", subquery$cdmTableName, " DROP CONSTRAINT IF EXISTS xpk_", subquery$cdmTableName, ";\n"))

  }

  # append pk for cohort_definition to fix bug in CommonDataModel code for v5.4
  if (cdmVersion == "5.4") {
    sql_result <- paste0(sql_result, "\nALTER TABLE @cdmDatabaseSchema.COHORT_DEFINITION DROP CONSTRAINT IF EXISTS xpk_COHORT_DEFINITION;\n")
  }

  return(paste0(sql_result, collapse = ""))
}


#' @describeIn formatDdl formatDropForeignKeys Returns a string containing the OHDSQL for deletion of foreign keys in the OMOP CDM.
#' @export
formatDropForeignKeys <- function(cdmVersion){

  # argument checks
  stopifnot(is.character(cdmVersion), length(cdmVersion) == 1, cdmVersion %in% listSupportedVersions())

  cdmFieldCsvLoc <- system.file(file.path("csv", paste0("OMOP_CDMv", cdmVersion, "_Field_Level.csv")), package = "CommonDataModel", mustWork = TRUE)
  cdmSpecs <- read.csv(cdmFieldCsvLoc, stringsAsFactors = FALSE)

  foreignKeys <- subset(cdmSpecs, isForeignKey == "Yes")
  foreignKeys$key <- paste0(foreignKeys$cdmTableName, "_", foreignKeys$cdmFieldName)

  sql_result <- c(paste0("--@targetDialect CDM Foreign Key Constraints for OMOP Common Data Model ", cdmVersion, "\n"))
  for (foreignKey in foreignKeys$key){

    subquery <- subset(foreignKeys, foreignKeys$key==foreignKey)

    # Fix bug in CommonDataModel v5.4
    if (cdmVersion == "5.4" && foreignKey == "COHORT_DEFINITION_cohort_definition_id") {
      subquery$cdmTableName <- "COHORT"
    }

    sql_result <- c(sql_result, paste0("\nALTER TABLE @cdmDatabaseSchema.", subquery$cdmTableName, " DROP CONSTRAINT IF EXISTS fpk_", subquery$cdmTableName, "_", subquery$cdmFieldName, ";\n"))

  }

  return(paste0(sql_result, collapse = ""))
}


#' @describeIn formatDdl formatDropIndexes Returns a string containing the OHDSQL for deletion of indexes in the OMOP CDM.
#' @export
formatDropIndexes <- function(cdmVersion){

  # argument checks
  stopifnot(is.character(cdmVersion), length(cdmVersion) == 1, cdmVersion %in% listSupportedVersions())

  cdmIndexCsvLoc <- system.file(file.path("csv", paste0("OMOP_CDMv", cdmVersion, "_Indexes.csv")), package = "CDMVocabulary", mustWork = TRUE)
  cdmIndexSpecs <- read.csv(cdmIndexCsvLoc, stringsAsFactors = FALSE)

  sql_result <- c(paste0("--@targetDialect CDM Indexes for OMOP Common Data Model ", cdmVersion, "\n"))
  cdmTableName <- ""
  for (rowNum in 1:nrow(cdmIndexSpecs)) {

    cdmIndexRow <- cdmIndexSpecs[rowNum,]

    # If new table, then add newline between create index statements
    if (cdmTableName != cdmIndexRow$cdmTableName) {
      sql_result <- c(sql_result, paste0("\n"))
      cdmTableName <- cdmIndexRow$cdmTableName
    }

    sql_result <- c(sql_result, paste0("DROP INDEX IF EXISTS ", cdmIndexRow$indexName, " ON @cdmDatabaseSchema.", tolower(cdmIndexRow$cdmTableName), ";\n"))

  }
  return(paste0(sql_result, collapse = ""))
}


#' @describeIn formatDdl formatTruncateVocab Returns a string containing the OHDSQL for truncation of vocabulary tables in the OMOP CDM.
#' @export
formatTruncateVocab <- function(cdmVersion){

  # argument checks
  stopifnot(is.character(cdmVersion), length(cdmVersion) == 1, cdmVersion %in% listSupportedVersions())

  tableList <- c("concept_ancestor", "concept_class", "concept_relationship", "concept_synonym",
                 "concept", "domain", "drug_strength", "relationship", "vocabulary")

  sql_result <- c(paste0("--@targetDialect CDM DDL Specification for OMOP Common Data Model ", cdmVersion))
  for (tableName in tableList){
    sql_result <- c(sql_result, paste0("TRUNCATE TABLE @cdmDatabaseSchema.", tableName, ";\n"))
  }
  return(paste0(sql_result, collapse = ""))
}
