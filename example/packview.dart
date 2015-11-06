import "dart:io";

import "package:realdb/msgpack.dart";

main(List<String> args) async {
  var file = new File(args[0]);
  print((await unpackFile(file)));
}
