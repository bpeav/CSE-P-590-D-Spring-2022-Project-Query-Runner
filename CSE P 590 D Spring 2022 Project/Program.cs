// See https://aka.ms/new-console-template for more information

using System.Xml;
using System.Xml.Linq;
using System.Xml.Schema;
using System.Xml.XPath;
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
        var queryStr = "SELECT MIN(chn.name) AS uncredited_voiced_character, MIN(t.title) AS russian_movie FROM char_name AS chn, cast_info AS ci, company_name AS cn, company_type AS ct, movie_companies AS mc, role_type AS rt, title AS t WHERE ci.note  like '%(voice)%' and ci.note like '%(uncredited)%' AND cn.country_code  = '[ru]' AND rt.role  = 'actor' AND t.production_year > 2005 AND t.id = mc.movie_id AND t.id = ci.movie_id AND ci.movie_id = mc.movie_id AND chn.id = ci.person_role_id AND rt.id = ci.role_id AND cn.id = mc.company_id AND ct.id = mc.company_type_id;";
        await _dbQueryRunner.RunSqlServerQuery(queryStr, cancellationToken).ConfigureAwait(false);
        var postgreSqlCardinalities = await _dbQueryRunner.RunPostgreSqlQuery(queryStr, cancellationToken).ConfigureAwait(false);
        var mySqlCardinalities = await _dbQueryRunner.RunMySqlQuery(queryStr, cancellationToken).ConfigureAwait(false);
        await _dbQueryRunner.RunMariaDbQuery(queryStr, cancellationToken).ConfigureAwait(false);
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }
}

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

    public async Task RunSqlServerQuery(string queryText, CancellationToken cancellationToken)
    {
        await using var connection = new SqlConnection(_databaseConnectionStringsOptions.Value.SqlServer);

        var gridReader = await connection
            .QueryMultipleAsync(new CommandDefinition(
                commandText: $"SET STATISTICS XML ON; {queryText}; SET STATISTICS XML OFF;",
                cancellationToken: cancellationToken))
            .ConfigureAwait(false);
        
        var queryResult = await gridReader.ReadAsync().ConfigureAwait(false);
        _logger.LogInformation("Output: {output}", System.Text.Json.JsonSerializer.Serialize(queryResult));
        
        var executionPlanAndStats = await gridReader.ReadAsync<string>().ConfigureAwait(false);
        var executionPlanXml = executionPlanAndStats.First();
        var parsedExecutionPlan = XDocument.Parse(executionPlanXml);
        _logger.LogInformation("Output: {output}", parsedExecutionPlan);
    }

    public async Task<List<(int estimated, int actual)>> RunPostgreSqlQuery(string queryText, CancellationToken cancellationToken)
    {
        await using var connection = new NpgsqlConnection(_databaseConnectionStringsOptions.Value.PostgreSql);

        var queryExplainAnalyze = await connection
            .QueryAsync<string>(new CommandDefinition(
                commandText: $"EXPLAIN ANALYZE {queryText}",
                cancellationToken: cancellationToken))
            .ConfigureAwait(false);
        
        _logger.LogInformation("Output: {output}", System.Text.Json.JsonSerializer.Serialize(queryExplainAnalyze));

        var list = new List<(int, int)>();

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

            if (!int.TryParse(startOfEstimatedRows.Substring(0, idx2), out var estimated))
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

            if (!int.TryParse(startOfRealRows.Substring(0, idx4), out var actual))
            {
                continue;
            }

            list.Add((estimated, actual));
        }

        return list;
    }

    public async Task<List<(int estimated, int actual)>> RunMySqlQuery(string queryText, CancellationToken cancellationToken)
    {
        await using var connection = new MySqlConnection(_databaseConnectionStringsOptions.Value.MySql);

        var queryExplainAnalyze = await connection
            .QuerySingleAsync<string>(new CommandDefinition(
                commandText: $"EXPLAIN ANALYZE {queryText}",
                cancellationToken: cancellationToken))
            .ConfigureAwait(false);
        
        _logger.LogInformation("Output: {output}", queryExplainAnalyze);

        var list = new List<(int, int)>();

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

            if (!int.TryParse(startOfEstimatedRows.Substring(0, idx2), out var estimated))
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

            if (!int.TryParse(startOfRealRows.Substring(0, idx4), out var actual))
            {
                break;
            }

            list.Add((estimated, actual));

            stringLeftToCheck = startOfRealRows;
        }

        return list;
    }

    public async Task RunMariaDbQuery(string queryText, CancellationToken cancellationToken)
    {
        await using var connection = new MySqlConnection(_databaseConnectionStringsOptions.Value.MariaDb);

        var queryExplainAnalyze = await connection
            .QuerySingleAsync<string>(new CommandDefinition(
                commandText: $"ANALYZE FORMAT=JSON {queryText}",
                cancellationToken: cancellationToken))
            .ConfigureAwait(false);
        
        _logger.LogInformation("Output: {output}", queryExplainAnalyze);
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