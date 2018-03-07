//part of econet_api;
import 'dart:async';
import 'dart:io';
import 'package:cron/cron.dart' as quartz;
import "package:threading/threading.dart";
import 'package:econet_api/econet_api.dart';
import 'package:path/path.dart' as p;
import 'package:watcher/watcher.dart';
import 'package:aws_sns/aws_dart.dart' as aws;
import 'dart:convert';
import 'package:http/http.dart' as http;
import 'dart:io';
import 'package:scribe/scribe.dart';
/// Bulk Enrollment Class is used to enroll the multiple equipments for the DR Event based on the mac addresses
/// Expects csv file as input
class WeatherCron extends RequestSink{

  static const String ConfigurationKey = "ConfigurationKey";
  static const String LoggingTargetKey = "LoggingTargetKey";
  WeatherCron(Map<String, dynamic> opts) : super(opts) {
    configuration = opts[ConfigurationKey];

    LoggingTarget target = opts[LoggingTargetKey];
    target?.bind(logger);

    //authenticationServer = new AuthServer<User, Token, AuthCode>(this);


    context = contextWithConnectionInfo(configuration.userDatabase);

    var emailClient = new aws.SESClient()
      ..secretKey = configuration.email.secretKey
      ..accessKey = configuration.email.accessKey;

    var notificationClient = new aws.SNSClient();
    notificationClient.secretKey = configuration.push.secretKey;
    notificationClient.accessKey = configuration.push.accessKey;
    notificationClient.onDisable.listen(endpointDisabled);
    configuration.push.configurations.forEach((key, app) {
      var platform = aws.PlatformApplication.platformForString(app.platform);
      notificationClient.platformApplications[key] = new aws.PlatformApplication(app.region, "${app.account}", platform, app.name);
    });

    notifier = new Notifier(notificationClient, emailClient, configuration.twilioServer, configuration.passwordResetBaseURL);

    equipmentStore = new EquipmentStore(
        configuration.equipmentDatabase.username, configuration.equipmentDatabase.password,
        configuration.equipmentDatabase.host, configuration.equipmentDatabase.port,
        configuration.equipmentDatabase.databaseName,
        configuration.healthServer);
    /*var cron = new quartz.Cron();
      WeatherController obj = new WeatherController(equipmentStore);
      print('every two minutes123');
      obj.weatherStartPointProducer(null);*/
  }
  EconetConfiguration configuration;
  ManagedContext context;
  var statusMap = new Map<String, dynamic>();
  Notifier notifier;
  EquipmentStore equipmentStore;
  //static const String LoggingTargetKey = "LoggingTargetKey";
  // Default Constructor
 /* WeatherCron() {
    configuration = new EconetConfiguration("config.yaml");
    context = contextWithConnectionInfo(configuration.userDatabase);
    var emailClient = new aws.SESClient()
      ..secretKey = configuration.email.secretKey
      ..accessKey = configuration.email.accessKey;


    var notificationClient = new aws.SNSClient();
    notificationClient.secretKey = configuration.push.secretKey;
    notificationClient.accessKey = configuration.push.accessKey;
    configuration.push.configurations.forEach((key, app) {
      var platform = aws.PlatformApplication.platformForString(app.platform);
      notificationClient.platformApplications[key] = new aws.PlatformApplication(app.region, "${app.account}", platform, app.name);
    });
    notifier = new Notifier(notificationClient, emailClient, configuration.twilioServer, configuration.passwordResetBaseURL);
    equipmentStore = new EquipmentStore(
        configuration.equipmentDatabase.username, configuration.equipmentDatabase.password,
        configuration.equipmentDatabase.host, configuration.equipmentDatabase.port,
        configuration.equipmentDatabase.databaseName,
        configuration.healthServer);

  }*/
  Future endpointDisabled(aws.PlatformApplicationEndpoint endpoint) async {
    try {
      var q = new Query<NotificationProfile>()
        ..matchOn.endpointArn = endpoint.asARN();
      await q.delete();
    } catch (_) {}
  }

  void setupRouter(Router router) {

  }

  // Postgres DB context
  ManagedContext contextWithConnectionInfo(
      DatabaseConnectionConfiguration connectionInfo) {
    var dataModel = new ManagedDataModel.fromPackageContainingType(
        this.runtimeType);
    var psc = new PostgreSQLPersistentStore.fromConnectionInfo(
        connectionInfo.username,
        connectionInfo.password, connectionInfo.host, connectionInfo.port,
        connectionInfo.databaseName);

    var ctx = new ManagedContext(dataModel, psc);
    ManagedContext.defaultContext = ctx;
    return ctx;
  }

  /// Enroll Method
  Future<String> start() async {
    try {

      var cron = new quartz.Cron();
      //print("test8");
      //cron.schedule(new quartz.Schedule.parse('*/59 * * * *'), () async {
        new Logger("aqueduct").severe("Cron Job is started.");
        weatherStartPointService(null);
    // });
      /*cron.schedule(new quartz.Schedule.parse("0 0 * ? * *"), () async {
       // cron.schedule(new quartz.Schedule.parse('0 0 0/1 1/1 * ? *'), () async {
        print('every two minutes1231');
        weatherStartPointService(null);
      });*/
      /*cron.schedule(new quartz.Schedule.parse('* 1 * * *'), () async {
        //WeatherControllerTest obj = new WeatherControllerTest(equipmentStore);

      }*/
      //});
    }
    catch(e){
      new Logger("aqueduct").severe("Exception occured while cron job for weather : " + e);
      //writeToLogFile("Exception occured while cron job for weather : " + e);
    }
     // obj.weatherStartPointProducer(null);
    //});
   //var cron = new quartz.Cron();

  }



  //WeatherController(this.equipmentStore);
  String minVersion="AC-RHUI-03-00-26";
  String clientId="WXUx29gnUy1UEnzHxC5Xu";
  String clientSecret="K8ac9XUDzTtpoUkSl2JwTlUsBP9LDeRAzDdhmNje";
  String filter="allstations";
  int threadPoolSize=4;
  var headers = {
    "Content-Type" : "application/json"
  };
  @httpGet

  /*Future<Response> getAllWeatherInformation() async {//() async {
   try {
     String fields;
     String filter;
     String clientId;
     String clientSecret;
     /*var locQ = new Query<Location>()

        ..matchOn.connectedSystems.includeInResultSet = true
        ..matchOn.hours.includeInResultSet=true
        ..matchOn.nestLocation.includeInResultSet = true
        ..matchOn.connectedSystems.matchOn.equipment.includeInResultSet = true;*/

    String weatherByteStream;
    for(int i=0;i<responseList.length;i++){
       Map weatherBatchMap=responseList[i];
       for (var zipCode in weatherBatchMap.keys) {
         weatherByteStream=getWeatherByteStream(weatherBatchMap[zipCode],zipCode);

         var weatherSelectQuery = new Query<Weather>()
           ..matchOn.zipCode =zipCode;
         var zipCodeCheck = await weatherSelectQuery.fetchOne();
         var weatherQuery;
         if(zipCodeCheck!=null) {
           weatherQuery = new Query<Weather>()
             ..matchOn.zipCode =zipCode
             ..values.weatherbytesstream = weatherByteStream;
           await weatherQuery.updateOne();
         }
         else {
           weatherQuery = new Query<Weather>()
             ..values.zipCode =zipCode
             ..values.weatherbytesstream = weatherByteStream;
           await weatherQuery.insert();
         }
           print("inserted");
        // }
       }
    }

       //return null;
       return new Response.ok("Success");
     }
   //}
    catch(e)
    {
    logger.warning("Error occured when fetching logs $e");
    return new Response.serverError();
    }
 // }
    //return false;
  }*/
  String getCondition(String value){
    if(value.indexOf("::")>-1){
      value=value.replaceAll(",","");
      if(value.length>5){
        var s1=value.split("::");
        value=s1[1];
      }
      else if(value.length<5){
        var s1=value.split("::");
        value=s1[1];
      }
      else if(value.length==5){
        var s1=value.split("::");
        value=s1[1];
      }
    }
    else if(value.indexOf(":")>-1){
      var s2=value.split(":");
      value= s2[2];
    }
    if(value=="CL"){
      return "1";
    }
    else if(value=="FW" || value=="SC"){
      return "0";
    }
    else if(value=="BK" || value=="OV"){
      return "2";
    }
    else if(value=="A" || value=="IP" || value=="RS" || value=="SI" || value=="WM"
        || value=="ZF"|| value=="ZL" || value=="ZR" || value=="ZY"){
      return "3";
    }
    else if(value=="R" || value=="RW" || value=="UP"){
      return "4";
    }
    else if(value=="BS"|| value=="S" || value=="SW"){
      return "5";
    }
    else if(value=="T" || value=="WP"){
      return "6";
    }
    else if(value=="BD" || value=="BN" || value=="BY"){
      return "7";
    }
    else if(value=="BR" || value=="L"){
      return "10";
    }
    else if(value=="F" || value=="H" || value=="IF"){
      return "11";
    }
    else if(value=="K"){
      return "12";
    }
    else if(value=="FR" || value=="VA"){
      return "13";
    }
    else if(value=="IC"){
      return "14";
    }
  }
  String formatValue(String value){
    if(value!=null && value.length<2){
      value="0"+value;
    }
    return value;
  }
  /*Future<bool> weatherDayDataInsert(var weatherDayForeCast,int day,String zipCode) async{
    var daySelectQuery = new Query<WeatherDayForecast>()
      ..matchOn.zipCode =zipCode;
    var zipCodeCheck = await daySelectQuery.fetchOne();
    var weatherQuery;
    if(zipCodeCheck!=null) {
      weatherQuery = new Query<WeatherDayForecast>()
        ..matchOn.zipCode =zipCode
        ..values.day = day
        ..values.condition = getCondition(weatherDayForeCast["cloudsCoded"]).toString()
        ..values.highTemp = double.parse(weatherDayForeCast["maxTempF"].toString())
        ..values.lowTemp = double.parse(weatherDayForeCast["minTempF"].toString());
      await weatherQuery.updateOne();
    }
    else {
      weatherQuery = new Query<WeatherDayForecast>()
        ..values.zipCode =zipCode
        ..values.day = day
        ..values.condition = getCondition(weatherDayForeCast["cloudsCoded"]).toString()
        ..values.highTemp = double.parse(weatherDayForeCast["maxTempF"].toString())
        ..values.lowTemp = double.parse(weatherDayForeCast["minTempF"].toString());
    await weatherQuery.insert();
    }
    return true;
  }
  Future<bool> weatherHourDataInsert(var weatherHourForeCast,int day,String zipCode) async{
    var hourSelectQuery = new Query<WeatherHourForecast>()
      ..matchOn.zipCode =zipCode;
    var zipCodeCheck = await hourSelectQuery.fetchOne();
    var weatherQuery;
    if(zipCodeCheck!=null) {
      weatherQuery = new Query<WeatherHourForecast>()
        ..matchOn.zipCode =zipCode
        ..values.condition = getCondition(weatherHourForeCast["cloudsCoded"]).toString()
        ..values.temp = double.parse(weatherHourForeCast["tempF"].toString())
        ..values.precipitation = weatherHourForeCast["precipIN"].toString();
      await weatherQuery.updateOne();
    }
    else {
      weatherQuery = new Query<WeatherHourForecast>()
        ..values.zipCode =zipCode
        ..values.condition = getCondition(weatherHourForeCast["cloudsCoded"]).toString()
        ..values.temp = double.parse(weatherHourForeCast["tempF"].toString())
        ..values.precipitation = weatherHourForeCast["precipIN"].toString();
      await weatherQuery.insert();
    }
    return true;
  }*/
  List<int> getWeatherByteStream( List weatherList,String zipCode){
    List<int> weatherByteStream=new List<int>(58);
    int n=0;
    for (int i=0;i<weatherList.length;i++) {
      try{
      if (i == 0) {
        if (weatherList[i]["ob"]["tempF"] != null) {
          weatherByteStream[n++] = toByte(
              num.parse(formatValue(weatherList[i]["ob"]["tempF"].toString())) +
                  40)[0];
        }
        if (weatherList[i]["ob"]["humidity"] != null) {
          weatherByteStream[n++] = toByte(num.parse(
              formatValue(weatherList[i]["ob"]["humidity"].toString())))[0];
        }
        if (weatherList[i]["ob"]["weatherPrimaryCoded"] != null) {
          weatherByteStream[n++] = toByte(num.parse(formatValue(getCondition(
              weatherList[i]["ob"]["weatherPrimaryCoded"].toString()))))[0];
        }
        if (weatherList[i]["ob"]["windSpeedMPH"] != null) {
          weatherByteStream[n++] = toByte(num.parse(
              formatValue(weatherList[i]["ob"]["windSpeedMPH"].toString())))[0];
        }
        List<int>currentList;
        if (weatherList[i]["ob"]["windDirDEG"] != null) {
          currentList = toByte(num.parse(
              formatValue(weatherList[i]["ob"]["windDirDEG"].toString())));
        }
        if (currentList.length > 1) {
          weatherByteStream[n++] = currentList[0];
          weatherByteStream[n++] = currentList[1];
        }
        else if (currentList != null && currentList.length == 1) {
          weatherByteStream[n++] = 00;
          weatherByteStream[n++] = currentList[0];
        }
      }
      if (i == 1) {
        for (int j = 0; j < weatherList[i][0]["periods"].length; j++) {
          if (weatherList[i][0]["periods"][j]["maxTempF"] != null) {
            weatherByteStream[n++] = toByte(num.parse(formatValue(
                weatherList[i][0]["periods"][j]["maxTempF"].toString())) +
                40)[0];
          }
          if (weatherList[i][0]["periods"][j]["minTempF"] != null) {
            weatherByteStream[n++] = toByte(num.parse(formatValue(
                weatherList[i][0]["periods"][j]["minTempF"].toString())) +
                40)[0];
          }
          if (weatherList[i][0]["periods"][j]["weatherPrimaryCoded"] != null) {
            weatherByteStream[n++] = toByte(num.parse(formatValue(getCondition(
                weatherList[i][0]["periods"][j]["weatherPrimaryCoded"]
                    .toString()))))[0];
          }
          if (weatherList[i][0]["periods"][j]["pop"] != null) {
            weatherByteStream[n++] = toByte(num.parse(formatValue(
                weatherList[i][0]["periods"][j]["pop"].toString())))[0];
          }
        }
      }
      if (i == 2) {
        for (int k = 0; k < weatherList[i][0]["periods"].length; k++) {
          if (weatherList[i][0]["periods"][k]["tempF"] != null) {
            weatherByteStream[n++] = toByte(num.parse(formatValue(
                weatherList[i][0]["periods"][k]["tempF"].toString())) + 40)[0];
          }
          // weatherByteStream[n++]=-1;
          if (weatherList[i][0]["periods"][k]["weatherPrimaryCoded"] != null) {
            weatherByteStream[n++] = toByte(num.parse(formatValue(getCondition(
                weatherList[i][0]["periods"][k]["weatherPrimaryCoded"]
                    .toString()))))[0];
          }
          if (weatherList[i][0]["periods"][k]["windSpeedMPH"] != null) {
            weatherByteStream[n++] = toByte(num.parse(formatValue(
                weatherList[i][0]["periods"][k]["windSpeedMPH"]
                    .toString())))[0];
          }
          List<int> hourList;
          if (weatherList[i][0]["periods"][k]["windDirDEG"] != null) {
            hourList = toByte(num.parse(formatValue(
                (weatherList[i][0]["periods"][k]["windDirDEG"].toString()))));
          }
          if (hourList != null && hourList.length > 1) {
            weatherByteStream[n++] = hourList[0];
            weatherByteStream[n++] = hourList[1];
          }
          else if (hourList != null && hourList.length == 1) {
            weatherByteStream[n++] = 00;
            weatherByteStream[n++] = hourList[0];
          }
          if (weatherList[i][0]["periods"][k]["pop"] != null) {
            weatherByteStream[n++] = toByte(num.parse(formatValue(
                weatherList[i][0]["periods"][k]["pop"].toString())))[0];
          }
        }
      }
    }
    catch(e)
    {
      new Logger("aqueduct").severe("Byte Stream Conversion error "+zipCode);
      //writeToLogFile();
    }
    }
    return weatherByteStream;
  }
  Future<Map> getWeatherBatchInformation(List<String> zipCodeList) async{
    String from="Today";
    String hourlyFilter="6hr";
    int dailyLimit=7;
    int hourlyLimit=4;
    var url = "https://api.aerisapi.com/batch?requests=";
    String currentWeatherFields="obTimestamp,ob.tempF,ob.humidity,ob.weatherPrimaryCoded,ob.windSpeedMPH,ob.windDir,ob.windDirDEG,ob.precipIN";
    int limit=7;
    String dailyFields="periods.timestamp,periods.maxTempF,periods.minTempF,periods.weatherPrimaryCoded,periods.pop";
    String hourFields="periods.timestamp,periods.tempF,periods.precipIN,%20periods.weatherPrimaryCoded,periods.windSpeedMPH,periods.windDir,periods.windDirDEG,periods.pop";
    var responseMap = new Map();
    //Map responseMap;
    String output="";
    for(int i=0;i<zipCodeList.length;i++){
      String currentWeather;
      if(i!=0){
        currentWeather=",/observations/"+zipCodeList[i]+"%3Ffields="+currentWeatherFields+"%26"+
            "filter="+filter+",";
      }
      else {
        currentWeather = "/observations/" + zipCodeList[i] +
            "%3Ffields=" + currentWeatherFields + "%26" +
            "filter=" + filter + ",";
      }
      String dailyForecast="/forecasts/"+zipCodeList[i]+"%3Flimit="+dailyLimit.toString()+"%26from="+
          from+"%26fields="+dailyFields+",";
      String hourForecast="/forecasts/"+zipCodeList[i]+"%3Ffilter="+hourlyFilter+
          "%26limit="+hourlyLimit.toString()+"%26from="+from+"%206am%26fields="+hourFields;
      output=output+currentWeather+dailyForecast+hourForecast;
    }
    url=url+output+"&client_id="+clientId+"&client_secret="+clientSecret;
    new Logger("aqueduct").info("Weather API URL="+url);
    var weatherResponse;
    try {
      var httpClient = new HttpClient();
      weatherResponse = await http.get(url, headers: headers).timeout(new Duration(seconds: 20));
      if (weatherResponse.statusCode != 200) {
        //print("Weather daily forecast api is not working.");
        new Logger("aqueduct").severe("Weather api is not working ");
        throw new HTTPResponseException(
            400, "Weather forecast api is not working.");
      }

    var valueMap = JSON.decode(weatherResponse.body);
    if(valueMap["response"].length==0){
      new Logger("aqueduct").severe("Weather api limit is crossed. ");
      throw new HTTPResponseException(
          400, "Weather api limit is crossed.");
    }
      //new Logger("aqueduct").info("Weather API value="+valueMap["request"]);
    String previousZipCode;
    List weatherList=new List();
    if(valueMap!=null && valueMap["response"]!=null && valueMap["response"].length>0 &&
        valueMap["response"]["responses"]!=null) {
      for (int i = 0; i < valueMap["response"]["responses"].length; i++) {
        if(i==valueMap["response"]["responses"].length-1){
          weatherList.add(valueMap["response"]["responses"][i]["response"]);
          responseMap[previousZipCode] = getZipList(weatherList);
          weatherList.clear();
        }
        else if (i != 0 &&
            previousZipCode != valueMap["response"]["responses"][i]["request"]
                .toString()
                .split("/")[2].split("?")[0]) {
          responseMap[previousZipCode] = getZipList(weatherList);
          weatherList.clear();
        }
        if(i!=valueMap["response"]["responses"].length-1) {
          weatherList.add(valueMap["response"]["responses"][i]["response"]);
          previousZipCode = valueMap["response"]["responses"][i]["request"]
              .toString()
              .split("/")[2].split("?")[0];
        }
      }
    }
    }catch(e){
      new Logger("aqueduct").severe("Weather api problem is coming "+e);
    }
    return responseMap;
  }

  /*Future<Map> getWeatherInformation(String zipCode) async{
    String fields="obTimestamp,ob.tempF,ob.humidity,ob.weatherCoded,ob.windSpeedMPH,ob.windDir,ob.windDirDEG,ob.precipIN,ob.cloudsCoded";

    var url = "http://api.aerisapi.com/observations/"+zipCode+"?fields="+fields+
        "&filter="+filter+"&client_id="+clientId+"&client_secret="+clientSecret;

    var weatherResponse = await http.get(url, headers: headers);
    if (weatherResponse.statusCode != 200) {
      throw new HTTPResponseException(400, "Weather api is not working.");
    }
    Map responseMap = JSON.decode(weatherResponse.body);
    return responseMap;
  }
  Future<Map> getDailyForecastInformation(String zipCode) async {
    String from="today";
    int limit=7;
    String fields="periods.timestamp,periods.maxTempF,periods.minTempF,periods.weatherPrimaryCoded,periods.cloudsCoded";
    var url = "http://api.aerisapi.com/forecasts/"+zipCode+"?limit="+limit.toString()+"&from="+
        from+"&fields="+fields+"&client_id="+clientId+"&client_secret="+clientSecret;
    var dailyForecastResponse = await http.get(
        url, headers: headers);
    if (dailyForecastResponse.statusCode != 200) {
      throw new HTTPResponseException(400, "Weather daily forecast api is not working.");

    }
    Map responseMap = JSON.decode(dailyForecastResponse.body);
    return responseMap;
  }
  Future<Map> getHourlyInformation(String zipCode) async {
    String from="Today 6am";
    int limit=4;
    String hourlyFilter="6hr";
    String fields="periods.timestamp,periods.tempF,periods.precipIN, periods.weatherPrimaryCoded,periods.cloudsCoded";
    var url = "http://api.aerisapi.com/forecasts/"+zipCode+"?hourlyFilter="+hourlyFilter+
        "&limit="+limit.toString()+"&from="+from+"&fields="+fields+"&client_id="+clientId+"&client_secret="+clientSecret;
    //var url = urlForIdentifiers(total,[hourlyFilter,limit,from,fields,clientId, clientSecret]);
    var hourlyForecastResponse = await http.get(
        url, headers: headers);
    if (hourlyForecastResponse.statusCode != 200) {
      print("Weather hour forecast api is not working.");
      throw new HTTPResponseException(400, "Weather hour forecast api is not working.");
    }
    Map responseMap = JSON.decode(hourlyForecastResponse.body);
    return responseMap;
    // return total;
  }*/
  String urlForIdentifiers(var total,List<dynamic> identifiers) {
    //var total = ["https://api.aerisapi.com"];
    total.addAll(identifiers.map((item) {
      if (item is String) {
        return item;
      }
      return "$item";
    }));
    return total.join("/");
  }
  /*Future<Response> getCurrentWeather() async {

    try {

      String fields;
      String filter;
      String clientId;
      String clientSecret;
      /*var locQ = new Query<Location>()

        ..matchOn.connectedSystems.includeInResultSet = true
        ..matchOn.hours.includeInResultSet=true
        ..matchOn.nestLocation.includeInResultSet = true
        ..matchOn.connectedSystems.matchOn.equipment.includeInResultSet = true;*/
      var weather = new Query<Weather>();
      //var locs = await locQ.fetch();apia
      var weatherResult=await weather.fetch();

      /* for(int i=0;i<locs.length;i++){
        for(int j=0;j<locs[i].equipment.length;j++) {
          if (checkSoftwareVersionApplicable(
              locs[i].equipment[j].softwareVersion, minVersion)) {
            String zipCode = locs[i].zipCode.toString();*/
      for(int i=0;i<weatherResult.length;i++){

        Map weatherResponseMap = await getWeatherInformation(weatherResult[i].zipCode);

        // var jsonString = weatherResponse;
        //Map data = JSON.decoapde(weatherResponse);
        print(weatherResponseMap["response"]["ob"]["tempF"]);
        // print(weatherResponse);
        Map weatherDailyMap = await getDailyForecastInformation(weatherResult[i].zipCode);
        print(weatherDailyMap["response"][0]["periods"][0]["maxTempF"]);
        /*var dailyForecastResponse = await http.get(
                dailyForecastUrl, headers: headers);*/
        //Map weatherHourMap = await getHourlyInformation(weatherResult[i].zipCode);
        // print(weatherHourMap["response"][0]["periods"][0]["timestamp"]);
        /*var hourlyForecastResponse = await http.get(
                hourlyForecastUrl, headers: headers);*/
        // print(hourlyForecastResponse["response"]);
        // }
        //}
      }
      return null;
      //return response.statusCode == 200 || response.statusCode == 404;
    } catch (e) {//logger.warning("Error occured when fetching logs $e");
    return new Response.serverError();}

    //return false;
  }
  List<int> getByte(String source){
    List<int> bytes = source
        .substring(1, source.length - 1)
        .split("-")
        .map((twoByteCode) => int.parse(twoByteCode, radix: 16))
        .toList();
  }*/
  List<int> toByte(int number){
    List<int> result = [ ];
    List finalList;
    try {
      var mask = 255; // 1111 1111 binary
      if (number == 0) {
        result.add(0);
      }
      while (number > 0) {
        var byte = number & mask;
        result.add(byte);
        number >>= 8; // shift 8x from left to right
      }
      finalList = new List();
      for (int a = result.length - 1; a > -1; a--) {
        finalList.add(result[a]);
      }
    }
    catch(e){
      new Logger("aqueduct").severe("Weather api Byte Stream problem is coming. "+e);
    }
    return finalList;

    // return result;
  }
  @override
  void didOpen() {
    var cron = new quartz.Cron();
    new Logger("aqueduct").info("Pipeline open, server is now responding.");
    bulkInsertInWeather();
    //new Logger("aqueduct").info("PipeLine Op");
    cron.schedule(new quartz.Schedule.parse('*/60 * * * *'), () async {
    new Logger("aqueduct").info("Cron Job is started.");
    weatherStartPointService(null);
     });

  }
 /* weatherStartPoint() async {
    var length = 4;
    var buffer = new _BoundedBuffer(length);
    var total = length * 2;
    var consumed = 0;
    var produced = 0;
    var threads = <Thread>[];
    var weather = new Query<Weather>();
    //var locs = await locQ.fetch();apia
    var weatherResult = await weather.fetch();

    /* for(int i=0;i<locs.length;i++){
        for(int j=0;j<locs[i].equipment.length;j++) {
          if (checkSoftwareVersionApplicable(
              locs[i].equipment[j].softwareVersion, minVersion)) {
            String zipCode = locs[i].zipCode.toString();*/
    List<String> zipCodeList = new List();
    List responseList = new List();
    List zipCodeBatchList = new List();
    for (int i = 0; i < (weatherResult.length); i++) {
      try {
        zipCodeList.add(weatherResult[i].zipCode.toString());
        if (zipCodeList.length == 8 || (i != 0 && i % (length * 2) == 0)) {
          List tempList = new List();
          tempList = zipCodeList;
          zipCodeBatchList.add(getZipList(zipCodeList));
          //zipCodeBatchList.add(zipCodeList);
          /*Map weatherResponseMap = await getWeatherBatchInformation(zipCodeList);*/
          //zipCodeList==new List();
          zipCodeList.clear();
          //responseList.add(weatherResponseMap);
        }
        else if (i == weatherResult.length - 1 ||
            (i != 0 && i % (length * 2) == 0)) {
          zipCodeBatchList.add(getZipList(zipCodeList));
          zipCodeList.clear();
          //zipCodeList==new List();
          // Map weatherResponseMap = await getWeatherBatchInformation(zipCodeList);
          //responseList.add(weatherResponseMap);
        }
      }
      catch (e) {
        print(e);
      }

      if (i != 0 && (i+1) % (length * 2) == 0) {
        Map byteStreamMap;
        List byteStreamList = new List();
        List byteStreamMapList=new List();


        for (int k = 0; k < zipCodeBatchList.length; k++) {
          Map weatherResponseMap = await getWeatherBatchInformation(
              zipCodeBatchList[k]);
          for (var zipCode in weatherResponseMap.keys) {
            //for (var l = 0; l < total; i++) {
            //var thread = new Thread(() async {
            byteStreamMap = new Map();
            List<int> weatherByteStreamValue =
            getWeatherByteStream(weatherResponseMap[zipCode], zipCode);
            byteStreamMap[zipCode] = weatherByteStreamValue;
            //byteStreamList.add(byteStreamMap);

            var weatherUpdateQuery = new Query<Weather>()
              ..matchOn.zipCode = zipCode
              ..values.weatherbytestream = weatherByteStreamValue.toString();
            await weatherUpdateQuery.updateOne();
            byteStreamMapList.add(byteStreamMap);

            //await buffer.put(byteStreamMap);
            //print("${Thread.current.name}: => $i");
            produced++;
            // });

            /*thread.name = "Producer $k";
            print("producer: " + k.toString());
            threads.add(thread);
            // print()
            await thread.start();*/
            // }
          }
        }
        //for (var m = 0; m < total; i++) {
        // var thread = new Thread(() async {
        //var byteStreamMapWeather = await buffer.take();
        //for (var zipCodeWeather in byteStreamMap.keys) {
        //print("${Thread.current.name}: <= $x");
        /*var weatherSelectQuery = new Query<Weather>()
                ..matchOn.zipCode =zipCodeWeather;
              var zipCodeCheck = await weatherSelectQuery.fetchOne();
              var weatherQueryUpdate;
              if(zipCodeCheck!=null) {*/
        for(int a=0;a<byteStreamMapList.length;a++) {
          for (var keys in byteStreamMapList[a].keys) {
            var locationQuery = new Query<Location>()
              ..matchOn.connectedSystems.includeInResultSet = true
              ..matchOn.connectedSystems.matchOn.equipment
                  .includeInResultSet = true
              ..matchOn.connectedSystems.matchOn.equipment.matchOn.type = "HVAC"
              ..matchOn.zipCode = whereRelatedByValue(keys);
            var location = await locationQuery.fetch();

            for (int n = 0; n < location.length; n++) {
              for (int o = 0; o < location[n].equipment.length; i++) {
                List<int> bytes=byteStreamMapList[a][keys];
                Equipment equipment = location[n].equipment[o];
                try {
                  await equipmentStore.sendValuesToEquipment(
                      equipment, (EquipmentProxy proxy) {
                    proxy.setDynamicPropertyValue(
                        "weatherByteStream",
                        bytes);
                  }, requestID: null);
                }
                catch(e){
                  print(e);
                }
              }
            }
          }
        }

        /*}
              else {
                weatherQueryUpdate = new Query<Weather>()
                  ..values.zipCode =zipCodeWeather
                  ..values.weatherbytesstream = weatherByteStream;
                await weatherQueryUpdate.insert();
              }*/
        //}
        consumed++;
        // });

        /* thread.name = "Consumer $m";
          print("Consumer: "+m.toString());
          threads.add(thread);
          await thread.start();
        }

        for (var thread in threads) {
          await thread.join();
        }*/

        //}
      }
    }
  }
*/
  weatherStartPointService(List resultList) async {
    var length = 4;
    // var buffer = new _BoundedBuffer(length);
    var total = length * 2;
    int consumed = 0;
    int produced = 0;
    try{
    var threads = <Thread>[];
    List<String> zipCodeList = new List();
    List responseList = new List();
    List zipCodeBatchList = new List();
    if (resultList == null) {
      var weather = new Query<Weather>();
      // int threadPoolSize=4;
      //var locs = await locQ.fetch();apia
      var weatherResult = await weather.fetch();
      int consumerLength;
      /* for(int i=0;i<locs.length;i++){
        for(int j=0;j<locs[i].equipment.length;j++) {
          if (checkSoftwareVersionApplicable(
              locs[i].equipment[j].softwareVersion, minVersion)) {
            String zipCode = locs[i].zipCode.toString();*/

      consumerLength = weatherResult.length;
      for (int i = 0; i < (weatherResult.length); i++) {
        zipCodeList.add(weatherResult[i].zipCode.toString());
        if (zipCodeList.length == 8 || (i != 0 && i % (length * 2) == 0)) {
          List tempList = new List();
          tempList = zipCodeList;
          zipCodeBatchList.add(getZipList(zipCodeList));
          zipCodeBatchList.add(zipCodeList);
          /*Map weatherResponseMap = await getWeatherBatchInformation(zipCodeList);*/
          //zipCodeList==new List();
          zipCodeList.clear();
          //responseList.add(weatherResponseMap);
        }
        else if (i == weatherResult.length - 1 ||
            (i != 0 && i % (length * 2) == 0)) {
          zipCodeBatchList.add(getZipList(zipCodeList));
          zipCodeList.clear();
          //zipCodeList==new List();
          //zipCodeList==new List();f
          // Map weatherResponseMap = await getWeatherBatchInformation(zipCodeList);
          //responseList.add(weatherResponseMap);
        }
      }
    }
    else {
      zipCodeBatchList.add(resultList);
    }
    // if (i != 0 && (i+1) % (length * 2) == 0) {
    Map byteStreamMap;
    List byteStreamList = new List();
    List byteStreamMapList = new List();

    double size;
    // size = (zipCodeBatchList.length * 8) / 4;
    //for (int k = 0; k < zipCodeBatchList.length; k++) {
    List splitListValue = splitList(zipCodeBatchList);

    for (int p = 0; p < splitListValue.length; p++) {
      var thread = new Thread(() async {
        for (int q = 0; q < splitListValue[p].length; q++) {
          if (splitListValue[p][q].length > 0) {
            if (resultList != null) {
              var weatherOneQuery = new Query<Weather>()
                ..matchOn.zipCode = splitListValue[p][q][0];
              //..values.weatherbytestream = weatherByteStreamValue.toString();
              var zipCodeOne = await weatherOneQuery.fetchOne();
              if (zipCodeOne != null && zipCodeOne.weatherbytestream != null) {
                var locationQuery = new Query<Location>()
                  ..matchOn.connectedSystems.includeInResultSet = true
                  ..matchOn.connectedSystems.matchOn.equipment
                      .includeInResultSet = true
                  ..matchOn.connectedSystems.matchOn.equipment.matchOn
                      .type = "HVAC"
                  ..matchOn.zipCode = whereRelatedByValue(zipCodeOne.zipCode);
                var location = await locationQuery.fetch();

                for (int n = 0; n < location.length; n++) {
                  for (int o = 0; o < location[n].equipment.length; o++) {
                    List<int> bytes = convertToList(
                        zipCodeOne.weatherbytestream);
                    Equipment equipment = location[n].equipment[o];
                    try {
                      var equipmentValues = await equipmentStore.getEquipmentProxy(
                          location[n].equipment[o].connectedSystem.macAddress,
                          location[n].equipment[o].deviceAddress);
                      location[n].equipment[o].pairWithValues(equipmentValues);
                      if (location[n].equipment[o].type =="HVAC" && new Version().checkSoftwareVersionApplicable(
                          minVersion, location[n].equipment[o].softwareVersion)) {
                        await equipmentStore.sendValuesToEquipment(
                            equipment, (EquipmentProxy proxy) {
                          if (proxy.isConnected == true) {
                            proxy.setDynamicPropertyValue(
                                "weatherByteStream",
                                bytes);
                          }
                        }, requestID: null);
                      }

                    }
                    catch (e) {
                      logger.info("Equipment is not connected."+location[n].equipment[o].id.toString());
                    }
                  }
                }
                return;
              }
            }

            Map weatherResponseMap = await getWeatherBatchInformation(
                splitListValue[p][q]);
            for (var zipCode in weatherResponseMap.keys) {
              //for (var l = 0; l < total; i++) {

              byteStreamMap = new Map();
              List<int> weatherByteStreamValue =
              getWeatherByteStream(weatherResponseMap[zipCode], zipCode);
              byteStreamMap[zipCode] = weatherByteStreamValue;
              //byteStreamList.add(byteStreamMap);

              var weatherUpdateQuery = new Query<Weather>()
                ..matchOn.zipCode = zipCode
                ..values.weatherbytestream = weatherByteStreamValue.toString();
              await weatherUpdateQuery.updateOne();
              //byteStreamMapList.add(byteStreamMap);

              //for (var zipCodeWeather in byteStreamMap.keys) {
              var locationQuery = new Query<Location>()
                ..matchOn.connectedSystems.includeInResultSet = true
                ..matchOn.connectedSystems.matchOn.equipment
                    .includeInResultSet = true
                ..matchOn.connectedSystems.matchOn.equipment.matchOn
                    .type = "HVAC"
                ..matchOn.zipCode = whereRelatedByValue(zipCode);
              var location = await locationQuery.fetch();

              for (int n = 0; n < location.length; n++) {
                for (int o = 0; o < location[n].equipment.length; o++) {
                  List<int> bytes = weatherByteStreamValue;
                  Equipment equipment = location[n].equipment[o];
                  try {
                    var equipmentValues = await equipmentStore.getEquipmentProxy(
                        location[n].equipment[o].connectedSystem.macAddress,
                        location[n].equipment[o].deviceAddress);
                    location[n].equipment[o].pairWithValues(equipmentValues);
                    if (location[n].equipment[o].type =="HVAC" && new Version().checkSoftwareVersionApplicable(
                        minVersion, location[n].equipment[o].softwareVersion)) {
                      await equipmentStore.sendValuesToEquipment(
                          equipment, (EquipmentProxy proxy) {
                        if (proxy!=null && proxy.isConnected == true) {
                          proxy.setDynamicPropertyValue(
                              "weatherByteStream",
                              bytes);
                        }
                      }, requestID: null);
                    }
                  }
                  catch (e) {
                    logger.info("Equipment is not connected."+location[n].equipment[o].id.toString());
                  }
                }
              }
              //}

              //await buffer.put(byteStreamMap);
              //print("${Thread.current.name}: => $p");
              //produced++;
            }
          }
        }
      });

      thread.name = "Producer $p";
      //print("producer: " + p.toString());
      threads.add(thread);
      // print()
      await thread.start();
      //}
    }
    //}
    // }
    //for (var m = 0; m < total; i++) {
    /*sleep(const Duration(seconds:30));
    int consumerReminder=consumerLength%threadPoolSize;
    int consumerSize=(consumerLength/threadPoolSize).toInt();*/
    /*for(int r=0;r<splitListValue.length;r++){
      if(r==splitListValue.length-1){
        consumerSize=consumerSize+consumerReminder;
      }
      //consumerSize=-1;
        var thread = new Thread(() async {
          for(int s=0;s<consumerSize;s++) {
            var byteStreamMapWeather = await buffer.take().timeout(new Duration(seconds: 20));
            //for (var zipCodeWeather in byteStreamMap.keys) {
            //print("${Thread.current.name}: <= $x");
            /*var weatherSelectQuery = new Query<Weather>()
                ..matchOn.zipCode =zipCodeWeather;
              var zipCodeCheck = await weatherSelectQuery.fetchOne();
              var weatherQueryUpdate;
              if(zipCodeCheck!=null) {*/
            //for(int a=0;a<byteStreamMapList.length;a++) {

            //for (var keys in byteStreamMapList[a].keys) {
            for (var zipCodeWeather in byteStreamMapWeather.keys) {
              var locationQuery = new Query<Location>()
                ..matchOn.connectedSystems.includeInResultSet = true
                ..matchOn.connectedSystems.matchOn.equipment
                    .includeInResultSet = true
                ..matchOn.connectedSystems.matchOn.equipment.matchOn
                    .type = "HVAC"
                ..matchOn.zipCode = whereRelatedByValue(zipCodeWeather);
              var location = await locationQuery.fetch();

              for (int n = 0; n < location.length; n++) {
                for (int o = 0; o < location[n].equipment.length; o++) {
                  List<int> bytes = byteStreamMapWeather[zipCodeWeather];
                  Equipment equipment = location[n].equipment[o];
                  await equipmentStore.sendValuesToEquipment(
                      equipment, (EquipmentProxy proxy) {
                    proxy.setDynamicPropertyValue(
                        "weatherByteStream",
                        bytes);
                  }, requestID: null);
                }
              }
            }
            //}

            /*}
              else {
                weatherQueryUpdate = new Query<Weather>()
                  ..values.zipCode =zipCodeWeather
                  ..values.weatherbytesstream = weatherByteStream;
                await weatherQueryUpdate.insert();
              }*/
            //}
          }
        consumed++;
         });

         thread.name = "Consumer $r";
          print("Consumer: "+r.toString());
          threads.add(thread);
          await thread.start();
      // }
    }
    for(var thread in threads){
          await thread.join();
        }
*/
    //}
    //}
  }
  catch(e)
  {
    logger.info("Weather api problem is coming"+e);
  }
  }
  List<int> convertToList(String value){

    value=value.replaceAll("[","").replaceAll("]","");
    List splitValue=value.split(",");
    List<int> finalList=new List();
    for(int i=0;i<splitValue.length;i++){
      finalList.add(int.parse(splitValue[i]));
    }
    return finalList;
  }
  List splitList(List list){
    List chunks = [];
    try {

      //int threadPoolSize=4;
      int value = (list.length / threadPoolSize).toInt();
      if (value == 0) {
        value = 1;
      }
      for (var i = 0; i < list.length; i += value) {
        if (i + value > list.length) {
          chunks.add(list.sublist(i, list.length - 1));
        }
        else {
          chunks.add(list.sublist(i, i + value));
        }
      }
    }catch(e) {
      logger.info("Weather api Split List Problem is coming" + e);
    }
    return chunks;
    /*letters.fold([[]], (list, x) {
    return list.last.length == 2 ? (list..add([x])) : (list..last.add(x));
  });*/
  }
  List getZipList(List list) {
    List tempList=new List();
    for(int n=0;n<list.length;n++){
      tempList.add(list[n]);
    }
    return tempList;
  }

  bulkInsertInWeather() async{
    syncWeathaerTable();
    weatherStartPointService(null);
  }
  
  syncWeathaerTable() async{  
		var locationQuery = new Query<Location>()
      ..matchOn.connectedSystems.includeInResultSet = true
      ..matchOn.connectedSystems.matchOn.equipment
          .includeInResultSet = true
      ..matchOn.connectedSystems.matchOn.equipment.matchOn
          .type = "HVAC";
      //..matchOn.zipCode = whereRelatedByValue(zipCode);
    var location = await locationQuery.fetch();
    List zipCodeList=new List();
    for(int i=0;i<location.length;i++) {
      if (location[i].equipment!=null){
        for (int l = 0; l < location[i].equipment.length; l++) {
          var equipmentValues = await equipmentStore.getEquipmentProxy(
              location[i].equipment[l].connectedSystem.macAddress,
              location[i].equipment[l].deviceAddress);
          Equipment eq = location[i].equipment[l];
          eq.pairWithValues(equipmentValues);
          if(eq.softwareVersion!=null)
          {
          if (location[i].equipment[l].type == "HVAC" &&
              new Version().checkSoftwareVersionApplicable(
                  minVersion, eq.softwareVersion)) {
            var weatherSelectQuery = new Query<Weather>()
              ..matchOn.zipCode = location[i].zipCode;
            var weatherResult = await weatherSelectQuery.fetchOne();
            if (weatherResult == null) {
              var weatherInsertQuery = new Query<Weather>()
                ..values.zipCode = location[i].zipCode;
              await weatherInsertQuery.insert();
              zipCodeList.add(location[i].zipCode);
            } //this is cron job start
          }
          }
        }
    }

    }
  }
}

/*test() async {//() async {
    try {
      String fields;
      String filter;
      String clientId;
      String clientSecret;
      /*var locQ = new Query<Location>()

        ..matchOn.connectedSystems.includeInResultSet = true
        ..matchOn.hours.includeInResultSet=true
        ..matchOn.nestLocation.includeInResultSet = true
        ..matchOn.connectedSystems.matchOn.equipment.includeInResultSet = true;*/
      var weather = new Query<Weather>();
      //var locs = await locQ.fetch();apia
      var weatherResult = await weather.fetch();

      /* for(int i=0;i<locs.length;i++){
        for(int j=0;j<locs[i].equipment.length;j++) {
          if (checkSoftwareVersionApplicable(
              locs[i].equipment[j].softwareVersion, minVersion)) {
            String zipCode = locs[i].zipCode.toString();*/
      List<String> zipCodeList = new List();
      List responseList = new List();
      for (int i = 0; i < (weatherResult.length); i++) {
        zipCodeList.add(weatherResult[i].zipCode.toString());
        if (zipCodeList.length == 8) {
          Map weatherResponseMap = await getWeatherBatchInformation(zipCodeList);
          zipCodeList.clear();
          responseList.add(weatherResponseMap);
        }
        else if(i==weatherResult.length-1) {
          Map weatherResponseMap = await getWeatherBatchInformation(zipCodeList);
          responseList.add(weatherResponseMap);
        }
      }
      String weatherByteStream;
      for(int i=0;i<responseList.length;i++){
        Map weatherBatchMap=responseList[i];
        for (var zipCode in weatherBatchMap.keys) {
          weatherByteStream=getWeatherByteStream(weatherBatchMap[zipCode],zipCode);

          var weatherSelectQuery = new Query<Weather>()
            ..matchOn.zipCode =zipCode;
          var zipCodeCheck = await weatherSelectQuery.fetchOne();
          var weatherQuery;
          if(zipCodeCheck!=null) {
            weatherQuery = new Query<Weather>()
              ..matchOn.zipCode =zipCode
              ..values.weatherbytesstream = weatherByteStream;
            await weatherQuery.updateOne();
          }
          else {
            weatherQuery = new Query<Weather>()
              ..values.zipCode =zipCode
              ..values.weatherbytesstream = weatherByteStream;
            await weatherQuery.insert();
          }
          print("inserted");
          // }
        }
      }

      //return null;
      //return new Response.ok("Success");
    }
    //}
    catch(e)
    {
      logger.warning("Error occured when fetching logs $e");
      return new Response.serverError();
    }
    // }
    return false;
  }*/
