// See https://aka.ms/new-console-template for more information

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

        services.AddOptions<DatabaseConnectionStrings>().BindConfiguration("DatabaseConnectionStrings");

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

            var sqlServerData = sqlServerCardinalities.Select(c => new JobQueryCardinalityInfo(jobQueryId, DbSystemConsts.SqlServer, c.estimated, c.actual));
            var postgreSqlData = postgreSqlCardinalities.Select(c => new JobQueryCardinalityInfo(jobQueryId, DbSystemConsts.PostgreSql, c.estimated, c.actual));
            var mySqlData = mySqlCardinalities.Select(c => new JobQueryCardinalityInfo(jobQueryId, DbSystemConsts.MySql, c.estimated, c.actual));
            var mariaDbData = mariaDbCardinalities.Select(c => new JobQueryCardinalityInfo(jobQueryId, DbSystemConsts.MariaDb, c.estimated, c.actual));

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

public record JobQueryCardinalityInfo(string JobQueryId, string Database, double EstimatedCardinality, double ActualCardinality);

public class DbQueryRunner
{
    private readonly ILogger<DbQueryRunner> _logger;
    private readonly IOptions<DatabaseConnectionStrings> _databaseConnectionStringsOptions;

    public DbQueryRunner(ILogger<DbQueryRunner> logger,
        IOptions<DatabaseConnectionStrings> _databaseConnectionStringsOptions)
    {
        _logger = logger;
        this._databaseConnectionStringsOptions = _databaseConnectionStringsOptions;
    }

    public async Task<List<(double estimated, double actual)>> RunSqlServerQuery(string queryText, CancellationToken cancellationToken)
    {
        await using var connection = new SqlConnection(_databaseConnectionStringsOptions.Value.SqlServer);

        var gridReader = await connection
            .QueryMultipleAsync(new CommandDefinition(
                commandText: $"SET STATISTICS PROFILE ON; {queryText}; SET STATISTICS PROFILE OFF;",
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