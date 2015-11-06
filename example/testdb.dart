import "dart:io";

import "package:realdb/realdb.dart";

const int count = 500000;

main() async {
  var db = new Database.locatedAt("testdb");
  var ids = [];
  await for (Row row in db.fetchTable("messages")) {
    if (ids.contains(row.id)) {
      print("FOUND DUPLICATE!!!!!");
      exit(1);
    }
    ids.add(row.id);
  }
  await db.close();
}
