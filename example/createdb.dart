import "dart:io";
import "dart:convert";

import "package:realdb/realdb.dart";

const int count = 10000;

main() async {
  var file = new File("example/data.json");
  var content = await file.readAsString();
  var json = JSON.decode(content);
  var dir = new Directory("testdb");
  if (await dir.exists()) {
    await dir.delete(recursive: true);
  }
  var db = new Database.locatedAt("testdb");
  await db.createTable("data");
  for (var a in json) {
    await db.insertIntoTable("data", a);
  }

  await db.flush();
  await db.close();
}
