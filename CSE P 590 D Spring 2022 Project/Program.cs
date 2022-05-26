// See https://aka.ms/new-console-template for more information

using System.Data;
using System.Globalization;
using CsvHelper;
using Dapper;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MySqlConnector;
using Npgsql;

var cts = new CancellationTokenSource();
await Host.CreateDefaultBuilder(args).ConfigureHostConfiguration(configHost =>
    {
        configHost.AddUserSecrets<Program>(false);
    })
    .ConfigureServices(services =>
    {
        services.AddHostedService<Worker>();

        services.AddOptions<DatabaseConnectionStrings>()
            //.BindConfiguration("DatabaseConnectionStrings");
            .BindConfiguration("AzureDatabaseConnectionStrings");

        services.AddSingleton<DbQueryRunner>();
    })
    .Build().RunAsync(cts.Token).ConfigureAwait(false);

public class Worker : IHostedService
{
    private readonly ILogger<Worker> _logger;
    private readonly DbQueryRunner _dbQueryRunner;

    public Worker(ILogger<Worker> logger, DbQueryRunner dbQueryRunner)
    {
        _logger = logger;
        _dbQueryRunner = dbQueryRunner;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        await _dbQueryRunner.CollectSqlServerStats(cancellationToken).ConfigureAwait(false);
        await _dbQueryRunner.CollectPostgreSqlStats(cancellationToken).ConfigureAwait(false);
        await _dbQueryRunner.CollectMySqlStats(cancellationToken).ConfigureAwait(false);
        await _dbQueryRunner.CollectMariaDbStats(cancellationToken).ConfigureAwait(false);


        const string jobQueryDir = "./JOB Queries";
        var jobQueryFilePaths = Directory.GetFiles(jobQueryDir);

        var skippedQueries = new List<string>();
        var cardinalityData = new List<JobQueryCardinalityInfo>();

        foreach (var jobQueryFilePath in jobQueryFilePaths)
        {
            var jobQueryId = Path.GetFileNameWithoutExtension(jobQueryFilePath);
            var queryStr = await File.ReadAllTextAsync(jobQueryFilePath, cancellationToken).ConfigureAwait(false);

            List<(double estimated, double actual)> sqlServerCardinalities, postgreSqlCardinalities, mySqlCardinalities, mariaDbCardinalities;

            try
            {
                sqlServerCardinalities = await _dbQueryRunner.RunSqlServerQuery(queryStr, cancellationToken).ConfigureAwait(false);
                postgreSqlCardinalities = await _dbQueryRunner.RunPostgreSqlQuery(queryStr, cancellationToken).ConfigureAwait(false);
                mySqlCardinalities = await _dbQueryRunner.RunMySqlQuery(queryStr, cancellationToken).ConfigureAwait(false);
                mariaDbCardinalities = await _dbQueryRunner.RunMariaDbQuery(queryStr, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Error occurred for Job Query {jobQueryId} - skipping this query in results", jobQueryId);
                skippedQueries.Add(jobQueryId);
                continue;
            }

            var sqlServerData = sqlServerCardinalities.Select(c => new JobQueryCardinalityInfo(jobQueryId, DbSystemConsts.SqlServer, c.estimated, c.actual, false));
            var postgreSqlData = postgreSqlCardinalities.Select(c => new JobQueryCardinalityInfo(jobQueryId, DbSystemConsts.PostgreSql, c.estimated, c.actual, false));
            var mySqlData = mySqlCardinalities.Select(c => new JobQueryCardinalityInfo(jobQueryId, DbSystemConsts.MySql, c.estimated, c.actual, false));
            var mariaDbData = mariaDbCardinalities.Select(c => new JobQueryCardinalityInfo(jobQueryId, DbSystemConsts.MariaDb, c.estimated, c.actual, false));

            cardinalityData.AddRange(sqlServerData);
            cardinalityData.AddRange(postgreSqlData);
            cardinalityData.AddRange(mySqlData);
            cardinalityData.AddRange(mariaDbData);
        }

        foreach (var jobQueryFilePath in jobQueryFilePaths)
        {
            var jobQueryId = Path.GetFileNameWithoutExtension(jobQueryFilePath);
            var queryStr = await File.ReadAllTextAsync(jobQueryFilePath, cancellationToken).ConfigureAwait(false);
            var fromStart = queryStr.IndexOf("FROM ");
            if (fromStart == -1)
            {
                continue;
            }

            var fromQueryOnly = queryStr.Substring(fromStart);
            var countQuery = "SELECT * " + fromQueryOnly;

            List<(double estimated, double actual)> sqlServerCardinalities, postgreSqlCardinalities, mySqlCardinalities, mariaDbCardinalities;

            try
            {
                sqlServerCardinalities = await _dbQueryRunner.RunSqlServerQuery(countQuery, cancellationToken).ConfigureAwait(false);
                postgreSqlCardinalities = await _dbQueryRunner.RunPostgreSqlQuery(countQuery, cancellationToken).ConfigureAwait(false);
                mySqlCardinalities = await _dbQueryRunner.RunMySqlQuery(countQuery, cancellationToken).ConfigureAwait(false);
                mariaDbCardinalities = await _dbQueryRunner.RunMariaDbQuery(countQuery, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Error occurred for Job Query {jobQueryId} - skipping this query in results", jobQueryId);
                skippedQueries.Add(jobQueryId);
                continue;
            }

            var sqlServerData = sqlServerCardinalities.Select(c => new JobQueryCardinalityInfo(jobQueryId, DbSystemConsts.SqlServer, c.estimated, c.actual, true));
            var postgreSqlData = postgreSqlCardinalities.Select(c => new JobQueryCardinalityInfo(jobQueryId, DbSystemConsts.PostgreSql, c.estimated, c.actual, true));
            var mySqlData = mySqlCardinalities.Select(c => new JobQueryCardinalityInfo(jobQueryId, DbSystemConsts.MySql, c.estimated, c.actual, true));
            var mariaDbData = mariaDbCardinalities.Select(c => new JobQueryCardinalityInfo(jobQueryId, DbSystemConsts.MariaDb, c.estimated, c.actual, true));

            cardinalityData.AddRange(sqlServerData);
            cardinalityData.AddRange(postgreSqlData);
            cardinalityData.AddRange(mySqlData);
            cardinalityData.AddRange(mariaDbData);
        }
        
        var testResultsDirectoryInfo = Directory.CreateDirectory("./TestResults/");
        var fileTimeString = DateTimeOffset.UtcNow.ToFileTime().ToString();

        await File.WriteAllTextAsync(Path.Combine(testResultsDirectoryInfo.FullName, $"{fileTimeString}_skipped_queries.txt"), string.Join(",\n", skippedQueries), cancellationToken).ConfigureAwait(false);

        await using var writer = new StreamWriter(Path.Combine(testResultsDirectoryInfo.FullName, $"{fileTimeString}_test_results.csv"));
        await using var csv = new CsvWriter(writer, CultureInfo.InvariantCulture);
        await csv.WriteRecordsAsync(cardinalityData, cancellationToken).ConfigureAwait(false);
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }
}

public static class DbSystemConsts
{
    public const string SqlServer = "MS SQL Server";
    public const string PostgreSql = "PostgreSQL";
    public const string MySql = "MySQL";
    public const string MariaDb = "MariaDB";
}

public record JobQueryCardinalityInfo(string JobQueryId, string Database, double EstimatedCardinality, double ActualCardinality, bool IsSelectStarQuery);

public class DbQueryRunner
{
    private readonly ILogger<DbQueryRunner> _logger;
    private readonly IOptions<DatabaseConnectionStrings> _databaseConnectionStringsOptions;
    
    private const int StatsCommandTimeoutInSeconds = 600;
    private const int QueryCommandTimeoutInSeconds = 120;

    public DbQueryRunner(ILogger<DbQueryRunner> logger,
        IOptions<DatabaseConnectionStrings> _databaseConnectionStringsOptions)
    {
        _logger = logger;
        this._databaseConnectionStringsOptions = _databaseConnectionStringsOptions;
    }

    public async Task CollectSqlServerStats(CancellationToken cancellationToken)
    {
        await using var connection = new SqlConnection(_databaseConnectionStringsOptions.Value.SqlServer);

        var commandResult = await connection
            .ExecuteAsync(new CommandDefinition(
                commandText: "EXEC sp_updatestats;",
                commandType: CommandType.StoredProcedure,
                commandTimeout:StatsCommandTimeoutInSeconds,
                cancellationToken: cancellationToken))
            .ConfigureAwait(false);
        
        _logger.LogInformation("Output: {output}", commandResult);
    }

    public async Task CollectPostgreSqlStats(CancellationToken cancellationToken)
    {
        await using var connection = new NpgsqlConnection(_databaseConnectionStringsOptions.Value.PostgreSql);

        var commandResult = await connection
            .ExecuteAsync(new CommandDefinition(
                commandText: "ANALYZE",
                commandTimeout:StatsCommandTimeoutInSeconds,
                cancellationToken: cancellationToken))
            .ConfigureAwait(false);
        
        _logger.LogInformation("Output: {output}", commandResult);
    }

    private static string[] _tableNames =
    {
        "aka_name",
        "aka_title",
        "cast_info",
        "char_name",
        "comp_cast_type",
        "company_name",
        "company_type",
        "complete_cast",
        "info_type",
        "keyword",
        "kind_type",
        "link_type",
        "movie_companies",
        "movie_info",
        "movie_info_idx",
        "movie_keyword",
        "movie_link",
        "name",
        "person_info",
        "role_type",
        "title"
    };

    public async Task CollectMySqlStats(CancellationToken cancellationToken)
    {
        await using var connection = new MySqlConnection(_databaseConnectionStringsOptions.Value.MySql);

        foreach (var tableName in _tableNames)
        {
            var commandResult = await connection
                .ExecuteAsync(new CommandDefinition(
                    commandText: $"ANALYZE TABLE {tableName}",
                    commandTimeout:StatsCommandTimeoutInSeconds,
                    cancellationToken: cancellationToken))
                .ConfigureAwait(false);

            _logger.LogInformation("Output: {output}", commandResult);
        }
    }

    public async Task CollectMariaDbStats(CancellationToken cancellationToken)
    {
        await using var connection = new MySqlConnection(_databaseConnectionStringsOptions.Value.MariaDb);

        foreach (var tableName in _tableNames)
        {
            var commandResult = await connection
                .ExecuteAsync(new CommandDefinition(
                    commandText: $"ANALYZE TABLE {tableName}",
                    commandTimeout:StatsCommandTimeoutInSeconds,
                    cancellationToken: cancellationToken))
                .ConfigureAwait(false);

            _logger.LogInformation("Output: {output}", commandResult);
        }
    }

    public async Task<List<(double estimated, double actual)>> RunSqlServerQuery(string queryText, CancellationToken cancellationToken)
    {
        await using var connection = new SqlConnection(_databaseConnectionStringsOptions.Value.SqlServer);

        var gridReader = await connection
            .QueryMultipleAsync(new CommandDefinition(
                commandText: $"SET STATISTICS PROFILE ON; {queryText}; SET STATISTICS PROFILE OFF;",
                commandTimeout:QueryCommandTimeoutInSeconds,
                cancellationToken: cancellationToken))
            .ConfigureAwait(false);
        
        var queryResult = await gridReader.ReadAsync().ConfigureAwait(false);
        _logger.LogInformation("Output: {output}", System.Text.Json.JsonSerializer.Serialize(queryResult));
        
        var executionPlanAndStats = await gridReader.ReadAsync().ConfigureAwait(false);
        _logger.LogInformation("Output: {output}", executionPlanAndStats);

        var cardinalities = executionPlanAndStats.Select(ep => ((double) ep.EstimateRows, (double) ep.Rows)).ToList();
        return cardinalities;
    }

    public async Task<List<(double estimated, double actual)>> RunPostgreSqlQuery(string queryText, CancellationToken cancellationToken)
    {
        await using var connection = new NpgsqlConnection(_databaseConnectionStringsOptions.Value.PostgreSql);

        var queryExplainAnalyze = await connection
            .QueryAsync<string>(new CommandDefinition(
                commandText: $"EXPLAIN ANALYZE {queryText}",
                commandTimeout:QueryCommandTimeoutInSeconds,
                cancellationToken: cancellationToken))
            .ConfigureAwait(false);
        
        _logger.LogInformation("Output: {output}", System.Text.Json.JsonSerializer.Serialize(queryExplainAnalyze));

        var list = new List<(double, double)>();

        foreach (var item in queryExplainAnalyze)
        {
            var idx = item.IndexOf("rows=");
            if (idx == -1)
            {
                continue;
            }

            var startOfEstimatedRows = item.Substring(idx + 5);
            var idx2 = startOfEstimatedRows.IndexOf(" ");
            if (idx2 == -1)
            {
                continue;
            }

            if (!double.TryParse(startOfEstimatedRows.Substring(0, idx2), out var estimated))
            {
                continue;
            }

            
            var idx3 = startOfEstimatedRows.IndexOf("rows=");
            if (idx3 == -1)
            {
                continue;
            }

            var startOfRealRows = startOfEstimatedRows.Substring(idx3 + 5);
            var idx4 = startOfRealRows.IndexOf(" ");
            if (idx4 == -1)
            {
                continue;
            }

            if (!double.TryParse(startOfRealRows.Substring(0, idx4), out var actual))
            {
                continue;
            }

            list.Add((estimated, actual));
        }

        return list;
    }

    public async Task<List<(double estimated, double actual)>> RunMySqlQuery(string queryText, CancellationToken cancellationToken)
    {
        await using var connection = new MySqlConnection(_databaseConnectionStringsOptions.Value.MySql);

        var queryExplainAnalyze = await connection
            .QuerySingleAsync<string>(new CommandDefinition(
                commandText: $"EXPLAIN ANALYZE {queryText}",
                commandTimeout:QueryCommandTimeoutInSeconds,
                cancellationToken: cancellationToken))
            .ConfigureAwait(false);
        
        _logger.LogInformation("Output: {output}", queryExplainAnalyze);

        var list = new List<(double, double)>();

        var stringLeftToCheck = queryExplainAnalyze;

        while (true)
        {
            var idx = stringLeftToCheck.IndexOf("rows=");
            if (idx == -1)
            {
                break;
            }

            var startOfEstimatedRows = stringLeftToCheck.Substring(idx + 5);
            var idx2 = startOfEstimatedRows.IndexOf(")");
            if (idx2 == -1)
            {
                break;
            }

            if (!double.TryParse(startOfEstimatedRows.Substring(0, idx2), out var estimated))
            {
                break;
            }

            
            var idx3 = startOfEstimatedRows.IndexOf("rows=");
            if (idx3 == -1)
            {
                break;
            }

            var startOfRealRows = startOfEstimatedRows.Substring(idx3 + 5);
            var idx4 = startOfRealRows.IndexOf(" ");
            if (idx4 == -1)
            {
                break;
            }

            if (!double.TryParse(startOfRealRows.Substring(0, idx4), out var actual))
            {
                break;
            }

            list.Add((estimated, actual));

            stringLeftToCheck = startOfRealRows;
        }

        return list;
    }

    public async Task<List<(double estimated, double actual)>> RunMariaDbQuery(string queryText, CancellationToken cancellationToken)
    {
        await using var connection = new MySqlConnection(_databaseConnectionStringsOptions.Value.MariaDb);

        var queryExplainAnalyzeResults = await connection
            .QueryAsync(new CommandDefinition(
                commandText: $"ANALYZE {queryText}",
                commandTimeout:QueryCommandTimeoutInSeconds,
                cancellationToken: cancellationToken))
            .ConfigureAwait(false);
        
        _logger.LogInformation("Output: {output}", queryExplainAnalyzeResults);

        var cardinalities = queryExplainAnalyzeResults.Select(q => ((double) q.rows, (double) q.r_rows)).ToList();
        return cardinalities;
    }
}

public class DatabaseConnectionStrings
{
    public DatabaseConnectionStrings() : this(string.Empty, string.Empty, string.Empty, string.Empty) {}
    public DatabaseConnectionStrings(string sqlServer, string postgreSql, string mySql, string mariaDb)
    {
        SqlServer = sqlServer;
        PostgreSql = postgreSql;
        MySql = mySql;
        MariaDb = mariaDb;
    }
    
    public string SqlServer { get; set; }
    public string PostgreSql { get; set; }
    public string MySql { get; set; }
    public string MariaDb { get; set; }
};