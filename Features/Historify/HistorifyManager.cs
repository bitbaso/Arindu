using System.Text;
using System.Text.Json;
using Arindu.Features.Historify.Entities;
using Arindu.Features.Historify.Interfaces;
using Dapper;
using MySql.Data.MySqlClient;

namespace Arindu.Features.Historify;

public class HistorifyManager(ILogger<HistorifyManager> logger) : IHistorifyManager
{
    #region  Private Properties
    #endregion Private Properties

    #region Public Methods

    /// <summary>
    /// Historify data
    /// </summary>
    /// <param name="connectionStringSource"></param>
    /// <param name="connectionStringDestination"></param>
    /// <param name="tablesToArchive"></param>
    /// <param name="batchSize"></param>
    /// <returns></returns>
    public async Task<bool> ExecuteHistorify(List<TableConfiguration> tablesToArchive)
    {
        try
        {
            var outcome = default(bool);

            if (tablesToArchive != null && tablesToArchive.Count > 0)
            {
                foreach (var tableConfig in tablesToArchive)
                {
                    if (tableConfig != null
                        && !string.IsNullOrEmpty(tableConfig.TableName)
                        && !string.IsNullOrEmpty(tableConfig.SchemaName)
                        && !string.IsNullOrEmpty(tableConfig.DestinationSchemaName)
                        && !string.IsNullOrEmpty(tableConfig.SourceConnectionString)
                        && !string.IsNullOrEmpty(tableConfig.DestinationConnectionString)
                        && tableConfig.DaysInterval > 0)
                    {
                        var sourceConnectionString = tableConfig.SourceConnectionString;
                        var destinationConnectionString = tableConfig.DestinationConnectionString;

                        await CreateSchema(tableConfig.DestinationSchemaName, sourceConnectionString);

                        using (var connectionSource = new MySqlConnection(sourceConnectionString))
                        {
                            using (var connectionDest = new MySqlConnection(destinationConnectionString))
                            {
                                connectionSource.Open();
                                connectionDest.Open();
                                var primaryKeys = await GetPrimaryKeysColumns(tableConfig.SchemaName,
                                                                              tableConfig.TableName,
                                                                              sourceConnectionString);

                                if (primaryKeys != null && primaryKeys.Count > 0)
                                {
                                    var createTableIfNotExistResult = await CreateTableIfNotExist(sourceConnectionString,
                                                                                                  destinationConnectionString,
                                                                                                  tableConfig.TableName);

                                    var hasMoreRows = true;

                                    while (hasMoreRows)
                                    {
                                        var rows = await GetRows(connectionSource,
                                                                 tableConfig.BatchSize,
                                                                 tableConfig.TableName,
                                                                 tableConfig.DateColumnName,
                                                                 tableConfig.DaysInterval);

                                        if (rows != null && rows.Count > 0)
                                        {
                                            hasMoreRows = true;

                                            var processBatchResult = await ProcessBatch(connectionSource,
                                                                                        connectionDest,
                                                                                        tableConfig.TableName,
                                                                                        primaryKeys,
                                                                                        rows);

                                        }
                                        else
                                        {
                                            hasMoreRows = false;
                                        }

                                    }
                                }
                            }
                        }

                        Console.WriteLine($"Historify completed {tableConfig.TableName}");


                        //Optimize table
                        var sourceOptimizeResult = await OptimizeTable(tableConfig.TableName, tableConfig.SourceConnectionString);
                        var destionationOptimizeResult = await OptimizeTable(tableConfig.TableName, tableConfig.DestinationConnectionString);
                    }
                    else
                    {
                        logger.LogInformation("Not tables configured");
                    }
                }

                outcome = true;
            }

            return outcome;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error executing Historify");
            return default;
        }
    }

    /// <summary>
    /// Copy the specified squema
    /// </summary>
    /// <param name="sourceConnectionString"></param>
    /// <returns></returns>
    public async Task<bool> CreateTableIfNotExist(string sourceConnectionString,
                                      string destinationConnectionString,
                                      string tableName)
    {
        try
        {
            var outcome = default(bool);

            if (sourceConnectionString != null
                && destinationConnectionString != null
                && !string.IsNullOrEmpty(tableName))
            {

                var tableExist = await TableExists(tableName, destinationConnectionString);
                if (!tableExist)
                {
                    using (var sourceConnection = new MySqlConnection(sourceConnectionString))
                    {
                        await sourceConnection.OpenAsync();

                        var dynamicResult = await sourceConnection.QueryFirstOrDefaultAsync<dynamic>($"SHOW CREATE TABLE `{tableName}`");

                        if (dynamicResult != null)
                        {
                            var fields = dynamicResult as IDictionary<string, object>;
                            var createTableSql = fields["Create Table"].ToString();
                            if (!string.IsNullOrEmpty(createTableSql))
                            {
                                outcome = await ExecuteStatementsAsync(createTableSql, destinationConnectionString);
                            }
                            else
                            {
                                logger.LogWarning($"Create Table empty {tableName}");
                            }

                        }
                        else
                        {
                            logger.LogWarning("No se encontró la tabla.");
                        }
                    }
                }
                else
                {
                    logger.LogInformation($"Table exists {tableName}");
                }
            }

            return outcome;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "ERROR CopySchemaAsync");
            return default;
        }
    }

    #endregion Public Methods

    #region Private Methods

    /// <summary>
    /// Create schema
    /// </summary>
    /// <param name="schemaName"></param>
    /// <param name="connectionString"></param>
    /// <returns></returns>
    private async Task<bool> CreateSchema(string schemaName, string connectionString)
    {
        try
        {
            var outcome = default(bool);

            if (!string.IsNullOrEmpty(schemaName) && !string.IsNullOrEmpty(connectionString))
            {
                logger.LogInformation($"Creating schema {schemaName}");
                var createDestinationSchemaSql = $"CREATE SCHEMA `{schemaName}` ;";

                outcome = await ExecuteStatementsAsync(createDestinationSchemaSql, connectionString);
            }

            return outcome;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "ERROR CreateSchema");
            return default;
        }
    }

    /// <summary>
    /// Gets the tables primary key columns
    /// </summary>
    /// <param name="schema"></param>
    /// <param name="tableName"></param>
    /// <param name="connectionString"></param>
    /// <returns></returns>
    private async Task<List<string>> GetPrimaryKeysColumns(string schema,
                                                            string tableName,
                                                            string connectionString)
    {
        try
        {
            var outcome = default(List<string>);

            if (!string.IsNullOrEmpty(schema)
                && !string.IsNullOrEmpty(tableName)
                && !string.IsNullOrEmpty(connectionString))
            {
                var getPrimaryKeysSql = @$"SELECT COLUMN_NAME AS ColumnName
                                        FROM information_schema.KEY_COLUMN_USAGE
                                        WHERE TABLE_SCHEMA = '{schema}'
                                        AND TABLE_NAME = '{tableName}'
                                        AND CONSTRAINT_NAME = 'PRIMARY';";

                using (var mysqlConnection = new MySqlConnection(connectionString))
                {
                    await mysqlConnection.OpenAsync();

                    var primaryKeysRows = await mysqlConnection.QueryAsync<PrimaryKeyEntity>(getPrimaryKeysSql);
                    if (primaryKeysRows != null && primaryKeysRows.Count() > 0)
                    {
                        outcome = new List<string>();
                        foreach (var primaryKeyRow in primaryKeysRows)
                        {
                            if (primaryKeyRow != null && !string.IsNullOrEmpty(primaryKeyRow.ColumnName))
                            {
                                outcome.Add(primaryKeyRow.ColumnName);
                            }
                        }
                    }
                }
            }

            return outcome;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "ERROR  GetPrimaryKeysColumns");
            return default;
        }
    }


    /// <summary>
    /// Execute sql
    /// </summary>
    /// <param name="sqlStatement"></param>
    /// <param name="connectionString"></param>
    /// <returns></returns>
    private async Task<bool> ExecuteStatementsAsync(string sqlStatement, string connectionString)
    {
        try
        {
            var outcome = default(bool);

            using (var destinationConnection = new MySqlConnection(connectionString))
            {
                await destinationConnection.OpenAsync();

                using (var transaction = destinationConnection.BeginTransaction())
                {
                    try
                    {
                        await destinationConnection.ExecuteAsync(sqlStatement, transaction: transaction);
                        transaction.Commit();
                        outcome = true;
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        logger.LogError($"Error executing statements: {ex.Message} - sql {sqlStatement}");
                    }
                }
            }

            return outcome;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "ERROR ExecuteStatementsAsync");
            return default;
        }
    }

    /// <summary>
    /// Check if table exists
    /// </summary>
    /// <param name="tableName"></param>
    /// <param name="connectionString"></param>
    /// <returns></returns>
    private async Task<bool> TableExists(string tableName, string connectionString)
    {
        try
        {
            var outcome = default(bool);

            using (var connection = new MySqlConnection(connectionString))
            {
                await connection.OpenAsync();

                var query = @"
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_schema = DATABASE()
                AND table_name = @TableName";

                var count = await connection.ExecuteScalarAsync<int>(query, new { TableName = tableName });
                outcome = count > 0;
            }

            return outcome;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, $"ERROR TableExistsAsync");
            return default;
        }
    }


    /// <summary>
    /// Process batch rows
    /// </summary>
    /// <param name="connectionStringSource"></param>
    /// <param name="connectionStringDestination"></param>
    /// <param name="tableName"></param>
    /// <param name="primaryKeys"></param>
    /// <param name="rows"></param>
    /// <returns></returns>
    public async Task<bool> ProcessBatch(MySqlConnection connectionStringSource,
                                         MySqlConnection connectionStringDestination,
                                         string tableName,
                                         List<string> primaryKeys,
                                         List<dynamic> rows)
    {
        try
        {
            var outcome = default(bool);

            if (connectionStringSource != null
                && connectionStringDestination != null
                && !string.IsNullOrEmpty(tableName)
                && primaryKeys != null
                && rows != null)
            {
                using (var transaction = connectionStringSource.BeginTransaction())
                {
                    try
                    {
                        var insertQuerys = await GenerateInsertQuery(tableName, rows);

                        if (!string.IsNullOrEmpty(insertQuerys))
                        {
                            await connectionStringDestination.ExecuteAsync(insertQuerys, transaction);
                        }

                        var primaryKeysCondition = await GeneratePrimaryKeyCondition(primaryKeys, rows);

                        var deleteRowsResult = await DeleteRows(connectionStringSource,
                                                                tableName,
                                                                primaryKeysCondition);

                        transaction.Commit();

                        outcome = true;
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        logger.LogError(ex, "Error ProcessBatch. Rollback");
                    }
                }

                logger.LogDebug($"ProcessBatchResult: {outcome} {tableName}");
            }
            else
            {
                logger.LogWarning($"Bad parameters {tableName}");
            }

            return outcome;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "ERROR  ProcessBatch");
            return default;
        }
    }

    /// <summary>
    /// Get rows by date column
    /// </summary>
    /// <param name="connection"></param>
    /// <param name="batchSize"></param>
    /// <param name="tableName"></param>
    /// <param name="dateColumnName"></param>
    /// <returns></returns>
    private async Task<List<dynamic>> GetRows(MySqlConnection connection,
                                      int batchSize,
                                      string tableName,
                                      string dateColumnName,
                                      int daysInterval)
    {
        try
        {
            var outcome = default(List<dynamic>);

            if (connection != null
                && batchSize > 0
                && !string.IsNullOrEmpty(tableName)
                && !string.IsNullOrEmpty(dateColumnName))
            {
                var selectQuery = $@"
                        SELECT *
                        FROM {tableName}
                        WHERE {dateColumnName} < NOW() - INTERVAL {daysInterval} DAY
                        LIMIT @BatchSize";

                logger.LogDebug($"GetRows. Executing SQL query: {selectQuery}");

                outcome = (await connection.QueryAsync(selectQuery, new { batchSize })).ToList();
            }
            else
            {
                logger.LogWarning($"GetRows. Bad parameters: {tableName} {dateColumnName} {batchSize} {connection}");
            }

            return outcome;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "ERROR GetRows");
            return default;
        }
    }

    /// <summary>
    /// Delete rows
    /// </summary>
    /// <param name="connection"></param>
    /// <param name="tableName"></param>
    /// <param name="primaryKeysCondition"></param>
    /// <returns></returns>
    private async Task<bool> DeleteRows(MySqlConnection connection,
                                      string tableName,
                                      string primaryKeysCondition)
    {
        try
        {
            var outcome = default(bool);

            if (connection != null
                && !string.IsNullOrEmpty(tableName)
                && !string.IsNullOrEmpty(primaryKeysCondition))
            {
                var deleteQuery = $@"
                            DELETE FROM {tableName}
                            WHERE {primaryKeysCondition}";

                logger.LogDebug($"DeleteRows. Executing SQL query: {deleteQuery}");

                var executeResult = await connection.ExecuteAsync(deleteQuery);
                if (executeResult > 0)
                {
                    outcome = true;
                }
            }
            else
            {
                logger.LogWarning($"DeleteRows. Bad parameters: {tableName} {primaryKeysCondition} {connection}");
            }

            return outcome;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "ERROR DeleteRows");
            return default;
        }
    }

    /// <summary>
    /// Optimize table
    /// </summary>
    /// <param name="tableName"></param>
    /// <param name="connectionString"></param>
    /// <returns></returns>
    private async Task<bool> OptimizeTable(string tableName, string connectionString)
    {
        try
        {
            var outcome = default(bool);

            if (!string.IsNullOrEmpty(connectionString) && !string.IsNullOrEmpty(tableName))
            {
                var optimizeQuery = $"OPTIMIZE TABLE {tableName}";

                logger.LogInformation($"Optimizing table {tableName}");

                using (var connSource = new MySqlConnection(connectionString))
                {
                    await connSource.ExecuteAsync(optimizeQuery);
                }

                outcome = true;
                logger.LogInformation($"Optimized table {tableName}");
            }
            else
            {
                logger.LogWarning($"OptimizeTable. Empty table {tableName}");
            }

            return outcome;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, $"ERROR OptimizeTable {tableName} {connectionString}");
            return default;
        }
    }

    /// <summary>
    /// Gets the insert querys
    /// </summary>
    /// <param name="tableName"></param>
    /// <param name="rows"></param>
    /// <returns></returns>
    private async Task<string> GenerateInsertQuery(string tableName, List<dynamic> rows)
    {
        try
        {
            var outcome = default(string);
            if (!string.IsNullOrEmpty(tableName)
                && rows != null
                && rows.Count > 0)
            {
                var firstRow = (IDictionary<string, object>)rows.First();
                if (firstRow != null
                    && firstRow.Keys != null
                    && firstRow.Keys.Count > 0)
                {
                    var columns = string.Join(", ", firstRow.Keys);
                    var values = new List<string>();

                    foreach (var row in rows)
                    {
                        if (row != null)
                        {
                            var rowValues = ((IDictionary<string, object>)row).Values.Select(v => FormatValue(v));
                            var rowValuesJoined = string.Join(", ", rowValues);
                            values.Add($"({rowValuesJoined})");
                        }
                    }

                    var valuesClause = string.Join(", ", values);
                    outcome = $"INSERT INTO {tableName} ({columns}) VALUES {valuesClause}";
                }

            }
            else
            {
                logger.LogWarning("GenerateInsertQuery. Bad parameters.");
            }

            return outcome;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "ERROR GenerateInsertQuery");
            return default;
        }
    }

    /// <summary>
    /// Formats a value for SQL insert query
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    private string FormatValue(object value)
    {
        try
        {
            var outcome = default(string);

            switch (value)
            {
                case null:
                    outcome = "NULL";
                    break;
                case string s:
                    outcome = $"'{s.Replace("'", "''")}'"; // Escapar comillas simples en cadenas de texto
                    break;
                case DateTime dt:
                    outcome = $"'{dt:yyyy-MM-dd HH:mm:ss}'"; // Formatear fecha
                    break;
                case bool b:
                    outcome = b ? "1" : "0"; // Formatear booleanos como 1 o 0
                    break;
                default:
                    outcome = value.ToString(); // Dejar los números y otros tipos como están
                    break;
            }

            return outcome;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "ERROR FormatValue");
            return default;
        }

    }

    /// <summary>
    /// Generates primary key condition
    /// </summary>
    /// <param name="primaryKeys"></param>
    /// <param name="rows"></param>
    /// <returns></returns>
    private async Task<string> GeneratePrimaryKeyCondition(List<string> primaryKeys, List<dynamic> rows)
    {
        try
        {
            var outcome = default(string);

            if (primaryKeys != null
                && primaryKeys.Count > 0
                && rows != null)
            {
                var conditions = rows.Select(row =>
                                  {
                                      var keyConditions = primaryKeys.Select(pk => $"{pk} = '{((IDictionary<string, object>)row)[pk]}'");
                                      return $"({string.Join(" AND ", keyConditions)})";
                                  });

                outcome = string.Join(" OR ", conditions);
            }
            else
            {
                logger.LogWarning($"GeneratePrimaryKeyCondition. Bad parameters: {primaryKeys} {rows}");
            }

            return outcome;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "ERROR GeneratePrimaryKeyCondition");
            return default;
        }

    }
    #endregion Private Methods
}
