# executes code to load/reload vocabulary tables
#
# assumptions:
#   latest

if (!require("devtools")) install.packages("devtools")
devtools::install_github("OHDSI/CommonDataModel", "v5.4")
library(CommonDataModel)
library(DatabaseConnector)

Sys.setenv("DATABASECONNECTOR_JAR_FOLDER" = "./jdbc")
downloadJdbcDrivers("sql server")


connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "dbms",
                                            server = "server",
                                            user = "user",
                                            password = "password")
dropConstraints(connectionDetails, "5.3", "cdm.dbo")
truncateVocabTables(connectionDetails, "5.3", "cdm.dbo")
loadVocabTables(connectionDetails, "cdm.dbo", "vocabulary")
reapplyConstraints(connectionDetails, "5.3", "cdm.dbo")



