import "package:realdb/realdb.dart";

import "dart:io";

main() async {
  var db = new Database.locatedAt("testdb", fileChunkSize: 1024);
  var size = await db.getTableSize("data");
  var i = 0;
  String msg = "Loaded 0 out of ${size} (0.0%)";
  stdout.write(msg);
  await for (Row row in db.fetchTable("data")) {
    i++;
    stdout.write("\r");
    var percent = ((i / size) * 100).toStringAsFixed(1);
    var m = "Loaded ${i} out of ${size} (${percent}%)";
    if (m.length < msg.length) {
      m = m.padRight(msg.length);
    }
    stdout.write(m);
    msg = m;
  }
  await db.close();
  stdout.writeln();
}
