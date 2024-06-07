library(DataQualityDashboard)

connectionDetails <- DatabaseConnector::createConnectionDetails(
  dbms     = "postgresql", 
  server   = "localhost/datoscat_tfm", 
  user     = "postgres", 
  password = "cafe", 
  port     = 5432, 
  pathToDriver = "/home/judith/.config/JetBrains/DataGrip2022.2/jdbc-drivers/PostgreSQL/42.6.0/org/postgresql/postgresql/42.6.0/"  
)

cdmDatabaseSchema <- "cdm"
vocabDatabaseSchema <- "vocabularies"
resultsDatabaseSchema <- "results"
cdmSourceName <- "TFM CDM"
cdmVersion <- "5.4"

numThreads <- 1
sqlOnly <- FALSE # set to TRUE if you just want to get the SQL scripts and not actually run the queries
sqlOnlyIncrementalInsert <- FALSE # set to TRUE if you want the generated SQL queries to calculate DQD results and insert them into a database table (@resultsDatabaseSchema.@writeTableName)
sqlOnlyUnionCount <- 1  # in sqlOnlyIncrementalInsert mode, the number of check sqls to union in a single query; higher numbers can improve performance in some DBMS (e.g. a value of 25 may be 25x faster)

outputFolder <- "output"
outputFile <- "results.json"

# logging type -------------------------------------------------------------------------------------
verboseMode <- TRUE # set to FALSE if you don't want the logs to be printed to the console

# write results to table? ------------------------------------------------------------------------------
writeToTable <- TRUE # set to FALSE if you want to skip writing to a SQL table in the results schema

# specify the name of the results table (used when writeToTable = TRUE and when sqlOnlyIncrementalInsert = TRUE)
writeTableName <- "dqdashboard_results"

# write results to a csv file? -----------------------------------------------------------------------
writeToCsv <- FALSE # set to FALSE if you want to skip writing to csv file
csvFile <- "" # only needed if writeToCsv is set to TRUE

# which DQ check levels to run -------------------------------------------------------------------
checkLevels <- c("TABLE", "FIELD", "CONCEPT")

# which DQ checks to run? ------------------------------------
checkNames <- c() 

# which CDM tables to exclude? ------------------------------------
# list of CDM table names to skip evaluating checks against; by default DQD excludes the vocab tables
tablesToExclude <- c("CONCEPT", "VOCABULARY", "CONCEPT_ANCESTOR", "CONCEPT_RELATIONSHIP", 
                     "CONCEPT_CLASS", "CONCEPT_SYNONYM", "RELATIONSHIP", "DOMAIN",
                     "LOCATION","CARE_SITE", "PROVIDER", 
                     "COHORT", "COHORT_DEFINITION", 
                     "COST", "PAYER_PLAN_PERIOD", 
                     "METADATA") 

# run the job --------------------------------------------------------------------------------------
DataQualityDashboard::executeDqChecks(connectionDetails = connectionDetails, 
                                      cdmDatabaseSchema = cdmDatabaseSchema, 
                                      resultsDatabaseSchema = resultsDatabaseSchema,
                                      vocabDatabaseSchema = vocabDatabaseSchema,
                                      cdmSourceName = cdmSourceName, 
                                      cdmVersion = cdmVersion,
                                      numThreads = numThreads,
                                      sqlOnly = sqlOnly, 
                                      sqlOnlyUnionCount = sqlOnlyUnionCount,
                                      sqlOnlyIncrementalInsert = sqlOnlyIncrementalInsert,
                                      outputFolder = outputFolder,
                                      outputFile = outputFile,
                                      verboseMode = verboseMode,
                                      writeToTable = writeToTable,
                                      writeToCsv = writeToCsv,
                                      csvFile = csvFile,
                                      checkLevels = checkLevels,
                                      tablesToExclude = tablesToExclude,
                                      checkNames = checkNames)

# inspect logs ----------------------------------------------------------------------------
# ParallelLogger::launchLogViewer(logFileName = file.path(outputFolder, sprintf("log_DqDashboard_%s.txt", cdmSourceName)))

# 
DataQualityDashboard::viewDqDashboard("/home/judith/output/results.json")

