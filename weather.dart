part of econet_api;

class Weather extends ManagedObject<_weather> implements _weather {

}

class _weather {
  @ManagedColumnAttributes(primaryKey: true)
  String zipCode;
  String weatherbytestream;
// ManagedSet<Equipment> equipmentEnableHistory;
}