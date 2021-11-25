# @file testReadr.R

processData <- function(tableName) {
  function(data, pos) {
    table <- as.data.frame(data)

    # Format Dates for tables that need it
    if (tolower(tableName) == "concept" || tolower(tableName) == "concept_relationship" || tolower(tableName) == "drug_strength") {
      table$valid_start_date <- as.Date(as.character(table$valid_start_date),"%Y%m%d")
      table$valid_end_date   <- as.Date(as.character(table$valid_end_date),"%Y%m%d")
    }

    for (i in 1:length(table)) {
      if (is.null(table[i, "concept_name"]) || nchar(table[i, "concept_name"]) == 0) {
        message(paste0("null concept_name for concept_id ", table[i, "concept_id"], ", row ", pos + i))
      }
    }
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

#' @title Tests processing vocabulary csv files
#' @description processFile Function to process vocabulary csv filessss.
#' @export
processFile <- function(cdmVersion, fileLoc, chunkSize = 10000)
{
  csvList <- c("concept.csv","vocabulary.csv","concept_ancestor.csv","concept_relationship.csv","relationship.csv","concept_synonym.csv","domain.csv","concept_class.csv", "drug_strength.csv")
  fileList <- list.files(fileLoc)
  fileList <- fileList[which(tolower(fileList) %in% csvList)]

  for (csv in fileList) {
    if (csv == "CONCEPT.csv") {
      writeLines(paste0("Loading: ", csv))
      tableName <- strsplit(csv,"[.]")[[1]][1]
      columnTypes <- getColumnTypes(cdmVersion, tableName)
      readr::read_tsv_chunked(paste0(fileLoc, "/", csv),
                              readr::DataFrameCallback$new(processData(tableName)),
                              col_types = columnTypes,
                              quote = "",
                              chunk_size = chunkSize)
    }
  }
}
