library realdb;

import "dart:async";
import "dart:convert";
import "dart:io";
import "dart:typed_data";

import "msgpack.dart";
import "id.dart";
import "io.dart";

import "package:crypto/crypto.dart";
import "package:path/path.dart" as pathlib;

class DatabaseException {
  final String message;

  DatabaseException(this.message);

  @override
  String toString() => message;
}

class Database {
  final Directory directory;
  final int opCacheSize;
  final int fileChunkSize;

  Database(this.directory, {this.opCacheSize: 5, this.fileChunkSize: 512});

  factory Database.locatedAt(String path) {
    return new Database(new Directory(path));
  }

  Future<List<String>> listTables() async =>
    await _load("tables.rl", []);

  Future createTable(String name) async {
    List<String> data = await _load("tables.rl", []);
    if (data.contains(name)) {
      throw new DatabaseException("Table '${name}' already exists.");
    }
    data.add(name);
    await _save("tables.rl", data);
    await _save("tables/${name}/table.rl", {
      "name": name,
      "created": new DateTime.now().millisecondsSinceEpoch
    });
  }

  Future insertIntoTable(String tableName, object) async {
    if (!(await doesTableExist(tableName))) {
      throw new DatabaseException("Table '${tableName}' does not exist.");
    }

    var packed = pack(object);
    var id = await _createRowHash(tableName, packed);

    var byteList = packed is! Uint8List ? new Uint8List.fromList(packed) : packed;
    var row = [id, byteList.length, byteList.buffer.asByteData()];

    _operations.add(new InsertOperation(tableName, row));
    await _checkOpsAndFlushIfNeeded();
    return true;
  }

  Future<bool> doesTableExist(String name) async =>
      (await listTables()).contains(name);

  Future buildObjectIndex(String tableName) async {
    if (!(await doesTableExist(tableName))) {
      throw new DatabaseException("Table '${tableName}' does not exist.");
    }

    var indexFile = _file("tables/${tableName}/index.rl");
    if (!(await indexFile.exists())) {
      await indexFile.writeAsBytes(pack({}));
    }

    var file = _file("tables/${tableName}/data.rl");

    if (!(await file.exists())) {
      return;
    }

    var index = await unpackListFileOnlyFirstValueAndPosition(file, chunkSize: fileChunkSize);
    await indexFile.writeAsBytes(pack(index));
  }

  Future<TableInfo> getTableInfo(String name) async {
    if (!(await doesTableExist(name))) {
      throw new DatabaseException("Table '${name}' does not exist.");
    }
    var json = await _load("tables/${name}/table.rl");
    return new TableInfo(json["name"], json["created"]);
  }

  Future<dynamic> _load(String path, [defaultValue = const {}]) async {
    var file = _file(path);
    if (!(await file.exists())) {
      return defaultValue;
    }
    var data = await unpackFile(file, chunkSize: fileChunkSize);
    return data;
  }

  Future _save(String path, object) async {
    var file = _file(path);
    if (!(await file.exists())) {
      await file.create(recursive: true);
    }
    await file.writeAsBytes(pack(object));
  }

  Stream<Row> fetchTable(String name, {bool unpackData: true}) async* {
    var file = _file("tables/${name}/data.rl");
    var raf = await file.open();

    var watch = new Stopwatch();
    var counts = [];

    try {
      var stream = new ChunkedFileByteStream(fileChunkSize, raf);

      var unpacker = new AsyncUnpacker(stream);
      await stream.skip(1);
      var len = await unpacker.unpackU32();
      for (var i = 0; i < len; i++) {
        watch.start();
        List data = await unpacker.unpack();
        var id = data[0];
        if (unpackData) {
          ByteData real = data[2];
          var rstream = new ByteDataStream(real);
          var runpacker = new AsyncUnpacker(rstream);
          var row = new Row(id, await runpacker.unpack());
          yield row;
        } else {
          var row = new Row(id, data[2]);
          yield row;
        }
        watch.stop();
        counts.add(watch.elapsedMicroseconds);
        watch.reset();
      }
    } finally {
      raf.close();
    }
  }

  Future flush() async {
    var inserts = <InsertOperation>[];
    while (_operations.isNotEmpty) {
      Operation op = _operations.removeAt(0);
      if (op is InsertOperation) {
        inserts.add(op);
      }
    }

    {
      var map = <String, List<dynamic>>{};

      while (inserts.isNotEmpty) {
        var op = inserts.removeAt(0);
        if (!map.containsKey(op.table)) {
          map[op.table] = [];
        }

        map[op.table].add(op.row);
      }

      for (String table in map.keys) {
        var file = _file("tables/${table}/data.rl");
        if (!(await file.exists())) {
          await file.writeAsBytes(pack([]));
        }
        await insertIntoListFile(file, map[table], multiple: true);
      }
      map = null;
    }
  }

  Future close() async {
    await flush();
  }

  Future _checkOpsAndFlushIfNeeded() async {
    if (_operations.length >= opCacheSize) {
      await flush();
    }
  }

  File _file(String path) => new File(pathlib.join(directory.path, path));

  List<Operation> _operations = [];
}

class TableInfo {
  final String name;
  final int created;

  DateTime get createdAt => new DateTime.fromMillisecondsSinceEpoch(created);

  TableInfo(this.name, this.created);
}

class Row {
  final String id;
  final dynamic object;

  Row(this.id, this.object);
}

Future<String> _createRowHash(String tableName, packed, {time, String salt, bool simple: false}) async {
  if (simple) {
    return await generateStrongToken(length: 16);
  }

  if (time == null) {
    time = new DateTime.now().millisecondsSinceEpoch;
  }

  if (time is String) {
    time = DateTime.parse(time).millisecondsSinceEpoch;
  }

  if (time is DateTime) {
    time = time.millisecondsSinceEpoch;
  }

  if (salt == null) {
    salt = await generateStrongToken(length: 10);
  }

  tableName = _createHash(tableName);
  packed = _createHash(packed);
  time = _createHash(time.toString());
  salt = _createHash(salt);
  return _createHash("${tableName}-${packed}-${time}-${salt}");
}

String _createHash(input) {
  if (input is String) {
    input = const Utf8Encoder().convert(input);
  }

  var sha = new SHA1();
  sha.add(input);
  return CryptoUtils.bytesToHex(sha.close());
}

abstract class Operation {
}

class InsertOperation extends Operation {
  final String table;
  final List<dynamic> row;

  InsertOperation(this.table, this.row);
}
