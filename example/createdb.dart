import "dart:io";

import "package:realdb/realdb.dart";

const int count = 50000;

main() async {
  var dir = new Directory("testdb");
  if (await dir.exists()) {
    await dir.delete(recursive: true);
  }
  var db = new Database.locatedAt("testdb");
  await db.createTable("messages");
  for (var n = 1; n <= count; n++) {
    await db.insertIntoTable("messages", {
      "message": "#${n}"
    });
  }

  await db.flush();
  await for (Row row in db.fetchTable("messages")) {
    print(row.id);
  }
  await db.buildObjectIndex("messages");
  await db.close();
}
