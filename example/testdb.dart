import "package:realdb/realdb.dart";

import "dart:io";

main() async {
  var db = new Database.locatedAt("testdb", fileChunkSize: 1024);
  var size = await db.getTableSize("data");
  var i = 0;
  String msg = "(0.0%) Loaded 0 out of ${size} (0.0ms)";
  stdout.write(msg);
  var watch = new Stopwatch();
  watch.start();
  await for (Row row in db.fetchTable("data")) {
    i++;
    var percent = ((i / size) * 100).toStringAsFixed(1);
    var m = "(${percent}%) Loaded ${i} out of ${size} (${watch.elapsedMicroseconds / 1000}ms)";
    if (m.length < msg.length) {
      m = m.padRight(msg.length);
    }
    stdout.write("\r${m}");
    msg = m;
  }
  watch.stop();
  await db.close();
  stdout.writeln();
}
