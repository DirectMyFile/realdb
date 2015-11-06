import "dart:io";
import "package:realdb/io.dart";

main() async {
  var file = new File("testdb/tables/messages/data.rl");
  var raf = await file.open();
  var waf = await file.open();

  var a = new ChunkedFileByteStream(10, raf);
  var b = new ChunkedFileByteStream(10, waf);

  await a.skip(1);
  await b.skip(1);
  print(await a.getPosition());
  print(await b.getPosition());
  print((await a.read()) == (await b.read()));

  await a.close();
  await b.close();
}