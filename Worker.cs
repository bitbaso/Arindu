using Arindu.Features.Historify.Entities;
using Arindu.Features.Historify.Interfaces;
using Arindu.Features.TestData.Interfaces;

namespace Arindu;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly IHistorifyManager _historifyManager;
    private readonly ITestDataManager _testDataManager;
    private readonly IConfiguration _configuration;

    public Worker(ILogger<Worker> logger,
                  IHistorifyManager historifyManager,
                  ITestDataManager testDataManager,
                  IConfiguration configuration)
    {
        _logger = logger;
        _historifyManager = historifyManager;
        _testDataManager = testDataManager;
        _configuration = configuration;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var loopTimeMilliseconds = _configuration.GetSection("LoopTimeMilliseconds").Get<int>();
        var tablesToHistorify = _configuration.GetSection("TablesConfiguration").Get<List<TableConfiguration>>();

        if (loopTimeMilliseconds > 0
            && tablesToHistorify != null
            && tablesToHistorify.Count > 0)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                if (_logger.IsEnabled(LogLevel.Information))
                {
                    _logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);
                }

                await _testDataManager.InsertRandomRowsAsync(10000, tablesToHistorify[0].SourceConnectionString);

                await _historifyManager.ExecuteHistorify(tablesToHistorify);

                await Task.Delay(loopTimeMilliseconds, stoppingToken);
            }
        }
    }
}
