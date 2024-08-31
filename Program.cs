using Arindu;
using Arindu.Features.Historify;
using Arindu.Features.Historify.Interfaces;
using Arindu.Features.TestData;
using Arindu.Features.TestData.Interfaces;
using Microsoft.Extensions.Logging;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddSingleton<ITestDataManager, TestDataManager>();

builder.Services.AddSingleton<IHistorifyManager, HistorifyManager>();

builder.Services.AddLogging(config =>
{
    config.ClearProviders();
    config.AddConsole();
    // Puedes agregar otros proveedores de logging aquí, como AddDebug, AddEventLog, etc.
});

builder.Services.AddHostedService<Worker>();

var host = builder.Build();
host.Run();
