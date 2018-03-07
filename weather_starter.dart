import 'dart:io';

import 'weatherCron.dart';
import 'package:cron/cron.dart' as quartz;
import "package:threading/threading.dart";
import 'dart:io';
import 'package:scribe/scribe.dart';
import 'package:aqueduct/aqueduct.dart';
import 'package:econet_api/econet_api.dart';
/// Bootstrap the Bulk Enrollment
main(List<String> args) async {
  var path = "${Directory.current.path}/batch_weather.log";
  var configuration = new EconetConfiguration("config.yaml");
  try {
    var logger = new LoggingServer([
      new RotatingLoggingBackend(path)
    ]);
    await logger.start();
    var app = new Application<WeatherCron>();
    app.configuration.configurationOptions = {
      WeatherCron.ConfigurationKey: configuration,
      WeatherCron.LoggingTargetKey: logger.getNewTarget()
    };
    //app.configuration.port = configuration.port;
    logger.getNewTarget().bind(app.logger);
    await app.start(numberOfInstances: 1);
  } on ApplicationSupervisorException catch (e) {
      print("${e.message}");
    }
}
