library msgpack;

import "dart:async";
import "dart:io";
import "dart:convert";
import "dart:typed_data";

import "io.dart";

List<int> pack(value) {
  return const Packer().pack(value);
}

class PackedListIterator {
  final ByteStream stream;

  PackedListIterator(this.stream);

  bool _initialized = false;

  init() async {
    if (_initialized) {
      return;
    }
    _initialized = true;
    await stream.skip(1);
    _length = await stream.readUint32();
    _unpacker = new AsyncUnpacker(stream);
  }

  int get length => _length;
  int _length = -1;

  int _pos = -1;
  int get index => _pos;

  bool get hasNext => _pos < length;

  Future<dynamic> read() async {
    init();
    if (_pos == length - 1) {
      throw new StateError("No more values left.");
    }
    return await _unpacker.unpack();
  }

  AsyncUnpacker _unpacker;
}

Future<dynamic> unpackFile(File file, {int chunkSize: 512}) async {
  var raf = await file.open();
  var stream = new ChunkedFileByteStream(chunkSize, raf);

  var unpacker = new AsyncUnpacker(stream);
  var result = await unpacker.unpack();
  await raf.close();
  return result;
}

Future<List<dynamic>> unpackListFileOnlyFirstValueInRow(File file, {int chunkSize: 512}) async {
  var raf = await file.open();
  var stream = new ChunkedFileByteStream(chunkSize, raf);

  var unpacker = new AsyncUnpacker(stream);
  var type = await stream.read();
  var len = await unpacker.unpackU32();
  var list = new List(len);
  for (var i = 0; i < len; i++) {
    await stream.skip(5);
    list[i] = await unpacker.unpack();
    var dataLength = await unpacker.unpack();
    await stream.skip(dataLength + 2);
  }
  await stream.close();
  return list;
}

Future<Map<dynamic, int>> unpackListFileOnlyFirstValueAndPosition(File file, {int chunkSize: 512}) async {
  var raf = await file.open();
  var stream = new ChunkedFileByteStream(chunkSize, raf);

  var unpacker = new AsyncUnpacker(stream);
  var type = await stream.read();
  var len = await unpacker.unpackU32();
  var map = {};
  for (var i = 0; i < len; i++) {
    await stream.skip(5);
    var key = await unpacker.unpack();
    var pos = await stream.getPosition();
    pos += 4;
    map[key] = pos;
    var dataLength = await unpacker.unpack();
    await stream.skip(dataLength + 2);
  }
  await stream.close();
  return map;
}

Future insertIntoListFile(File file, object, {bool isPacked: false, bool multiple: false}) async {
  var packed;
  if (isPacked) {
    packed = object;
  } else {
    if (multiple && object is List) {
      packed = object.map(pack).expand((x) => x).toList();
    } else {
      packed = pack(object);
    }
  }
  var raf = await file.open();
  var waf = await file.open(mode: FileMode.APPEND);
  var type = await raf.readByte();

  int len;

  if (type < 0xa0) {
    len = type - 0x90;
  } else if (type == 0xdd) {
    var list = new Uint8List.fromList(await raf.read(4));
    len = list.buffer.asByteData().getUint32(0);
  } else if (type == 0xdc) {
    var list = new Uint8List.fromList(await raf.read(2));
    len = list.buffer.asByteData().getUint16(0);
  } else {
    throw new Exception("Unsupported List Type");
  }

  raf.close();

  if (multiple) {
    len += object.length;
  } else {
    len++;
  }

  await waf.setPosition(0);
  await waf.writeByte(0xdd);
  await waf.writeFrom(Packer._encodeUint32(len));
  await waf.setPosition(await waf.length());
  await waf.writeFrom(packed);
  waf.close();
}

dynamic unpack(buffer) {
  if (buffer is TypedData) {
    buffer = buffer.buffer;
  }

  if (buffer is List) {
    buffer = new Uint8List.fromList(buffer).buffer;
  }

  if (_unpacker == null) {
    _unpacker = new Unpacker(buffer);
  } else {
    _unpacker.reset(buffer);
  }

  return _unpacker.unpack();
}

Unpacker _unpacker;

class Unpacker {
  ByteData data;
  int offset;

  Unpacker(ByteBuffer buffer, [this.offset = 0]) {
    data = new ByteData.view(buffer);
  }

  void reset(ByteBuffer buff) {
    data = new ByteData.view(buff);
    offset = 0;
  }

  unpack() {
    int type = data.getUint8(offset++);

    if (type >= 0xe0) return type - 0x100;
    if (type < 0xc0) {
      if (type < 0x80) return type;
      else if (type < 0x90) return unpackMap(() => type - 0x80);
      else if (type < 0xa0) return unpackList(() => type - 0x90);
      else return unpackString(() => type - 0xa0);
    }

    switch (type) {
      case 0xc0:
        return null;
      case 0xc2:
        return false;
      case 0xc3:
        return true;

      case 0xc4:
        return unpackBinary(type);
      case 0xc5:
        return unpackBinary(type);
      case 0xc6:
        return unpackBinary(type);

      case 0xcf:
        return unpackU64();
      case 0xce:
        return unpackU32();
      case 0xcd:
        return unpackU16();
      case 0xcc:
        return unpackU8();

      case 0xd3:
        return unpackS64();
      case 0xd2:
        return unpackS32();
      case 0xd1:
        return unpackS16();
      case 0xd0:
        return unpackS8();

      case 0xd9:
        return unpackString(unpackU8);
      case 0xda:
        return unpackString(unpackU16);
      case 0xdb:
        return unpackString(unpackU32);

      case 0xdf:
        return unpackMap(unpackU32);
      case 0xde:
        return unpackMap(unpackU16);
      case 0x80:
        return unpackMap(unpackU8);

      case 0xdd:
        return unpackList(unpackU32);
      case 0xdc:
        return unpackList(unpackU16);
      case 0x90:
        return unpackList(unpackU8);

      case 0xca:
        return unpackFloat32();
      case 0xcb:
        return unpackDouble();
    }
  }

  ByteData unpackBinary(int type) {
    int count;

    if (type == 0xc4) {
      count = data.getUint8(offset);
      offset += 1;
    } else if (type == 0xc5) {
      count = data.getUint16(offset);
      offset += 2;
    } else if (type == 0xc6) {
      count = data.getUint32(offset);
      offset += 4;
    } else {
      throw new Exception("Bad Binary Type");
    }

    var result = new ByteData(count);
    for (var i = 0; i < count; i++) {
      var idx = offset + i;
      result.setUint8(i, data.getUint8(idx));
    }
    offset += count;
    return result;
  }

  double unpackFloat32() {
    var value = data.getFloat32(offset);
    offset += 4;
    return value;
  }

  double unpackDouble() {
    var value = data.getFloat64(offset);
    offset += 8;
    return value;
  }

  int unpackU64() {
    int value = data.getUint64(offset);
    offset += 8;
    return value;
  }

  int unpackU32() {
    int value = data.getUint32(offset);
    offset += 4;
    return value;
  }

  int unpackU16() {
    int value = data.getUint16(offset);
    offset += 2;
    return value;
  }

  int unpackU8() {
    return data.getUint8(offset++);
  }

  int unpackS64() {
    int value = data.getInt64(offset);
    offset += 8;
    return value;
  }

  int unpackS32() {
    int value = data.getInt32(offset);
    offset += 4;
    return value;
  }

  int unpackS16() {
    int value = data.getInt16(offset);
    offset += 2;
    return value;
  }

  int unpackS8() {
    return data.getInt8(offset++);
  }

  String unpackString(int counter()) {
    var count = counter();
    String value = UTF8.decode(
      new List.from(new Uint8List.view(data.buffer, offset, count)));
    offset += count;
    return value;
  }

  Map unpackMap(int counter()) {
    var count = counter();
    Map map = {};
    for (int i = 0; i < count; ++i) {
      map[unpack()] = unpack();
    }
    return map;
  }

  List unpackList(int counter()) {
    var count = counter();
    List list = [];
    for (int i = 0; i < count; ++i) {
      list.add(unpack());
    }
    return list;
  }
}

class Packer {
  final bool useStandardList;

  const Packer({this.useStandardList: false});

  List<int> pack(value) {
    if (value == null) return const [0xc0];
    else if (value == false) return const [0xc2];
    else if (value == true) return const [0xc3];
    else if (value is int) return packInt(value);
    else if (value is String) return packString(value);
    else if (value is List) return packList(value);
    else if (value is Iterable) return packList(value.toList());
    else if (value is Map) return packMap(value);
    else if (value is double) return packDouble(value);
    else if (value is ByteData) return packBinary(value);
    throw new Exception("Failed to pack value: ${value}");
  }

  List<int> packAll(values) {
    List<int> encoded = [];
    for (var value in values)
      encoded.addAll(pack(value));
    return encoded;
  }

  Uint8List packBinary(ByteData bytes) {
    var count = bytes.elementSizeInBytes * bytes.lengthInBytes;

    if (count <= 255) {
      var out = new ByteData(count + 2);
      out.setUint8(0, 0xc4);
      out.setUint8(1, count);
      var i = 2;
      for (var b in bytes.buffer.asUint8List()) {
        out.setUint8(i, b);
        i++;
      }
      return out.buffer.asUint8List();
    } else if (count <= 65535) {
      var out = new ByteData(count + 3);
      out.setUint8(0, 0xc5);
      out.setUint16(1, count);
      var i = 2;
      for (var b in bytes.buffer.asUint8List()) {
        out.setUint8(i, b);
        i++;
      }
      return out.buffer.asUint8List();
    } else {
      var out = new ByteData(count + 5);
      out.setUint8(0, 0xc5);
      out.setUint32(1, count);
      var i = 2;
      for (var b in bytes.buffer.asUint8List()) {
        out.setUint8(i, b);
        i++;
      }
      return out.buffer.asUint8List();
    }
  }

  List<int> packInt(int value) {
    if (value < 128) {
      return [value];
    }

    List<int> encoded = [];
    if (value < 0) {
      if (value >= -32) encoded.add(0xe0 + value + 32);
      else if (value > -0x80) encoded.addAll([0xd0, value + 0x100]);
      else if (value > -0x8000) encoded
        ..add(0xd1)
        ..addAll(_encodeUint16(value + 0x10000));
      else if (value > -0x80000000) encoded
        ..add(0xd2)
        ..addAll(_encodeUint32(value + 0x100000000));
      else encoded
          ..add(0xd3)
          ..addAll(_encodeUint64(value));
    } else {
      if (value < 0x100) encoded.addAll([0xcc, value]);
      else if (value < 0x10000) encoded
        ..add(0xcd)
        ..addAll(_encodeUint16(value));
      else if (value < 0x100000000) encoded
        ..add(0xce)
        ..addAll(_encodeUint32(value));
      else encoded
          ..add(0xcf)
          ..addAll(_encodeUint64(value));
    }
    return encoded;
  }

  static List<int> _encodeUint16(int value) {
    return [(value >> 8) & 0xff, value & 0xff];
  }

  static List<int> _encodeUint32(int value) {
    return [
      (value >> 24) & 0xff,
      (value >> 16) & 0xff,
      (value >> 8) & 0xff,
      value & 0xff
    ];
  }

  static List<int> _encodeUint64(int value) {
    return [
      (value >> 56) & 0xff,
      (value >> 48) & 0xff,
      (value >> 40) & 0xff,
      (value >> 32) & 0xff,
      (value >> 24) & 0xff,
      (value >> 16) & 0xff,
      (value >> 8) & 0xff,
      value & 0xff
    ];
  }

  static const Utf8Encoder _utf8Encoder = const Utf8Encoder();

  List<int> packString(String value) {
    List<int> encoded = [];
    List<int> utf8 = _utf8Encoder.convert(value);
    if (utf8.length < 0x20) encoded.add(0xa0 + utf8.length);
    else if (utf8.length < 0x100) encoded.addAll([0xd9, utf8.length]);
    else if (utf8.length < 0x10000) encoded
      ..add(0xda)
      ..addAll(_encodeUint16(utf8.length));
    else encoded
        ..add(0xdb)
        ..addAll(_encodeUint32(utf8.length));
    encoded.addAll(utf8);
    return encoded;
  }

  List<int> packFloat(double value) {
    var f = new ByteData(5);
    f.setUint8(0, 0xca);
    f.setFloat32(1, value);
    return f.buffer.asUint8List();
  }

  List<int> packDouble(double value) {
    var f = new ByteData(9);
    f.setUint8(0, 0xcb);
    f.setFloat64(1, value);
    return f.buffer.asUint8List();
  }

  List<int> packList(List value) {
    List<int> encoded = [];
    if (useStandardList) {
      if (value.length < 16) encoded.add(0x90 + value.length);
      else if (value.length < 0x100) encoded
        ..add(0xdc)
        ..addAll(_encodeUint16(value.length));
      else encoded
          ..add(0xdd)
          ..addAll(_encodeUint32(value.length));
    } else {
      encoded
        ..add(0xdd)
        ..addAll(_encodeUint32(value.length));
    }
    for (var element in value) {
      encoded.addAll(pack(element));
    }
    return encoded;
  }

  List<int> packMap(Map value) {
    List<int> encoded = [];
    if (value.length < 16) encoded.add(0x80 + value.length);
    else if (value.length < 0x100) encoded
      ..add(0xde)
      ..addAll(_encodeUint16(value.length));
    else encoded
        ..add(0xdf)
        ..addAll(_encodeUint32(value.length));
    for (var element in value.keys) {
      encoded.addAll(pack(element));
      encoded.addAll(pack(value[element]));
    }
    return encoded;
  }
}

class StatefulPacker {
  final bool useStandardList;

  List<int> bytes = [];

  StatefulPacker({this.useStandardList: false});

  void pack(value) {
    if (value is Iterable && value is! List) {
      value = value.toList();
    }

    if (value == null) {
      bytes.add(0xc0);
    } else if (value == false) {
      bytes.add(0xc2);
    } else if (value == true) {
      bytes.add(0xc3);
    } else if (value is int) {
      packInt(value);
    } else if (value is String) {
      packString(value);
    } else if (value is List) {
      packList(value);
    } else if (value is Map) {
      packMap(value);
    } else if (value is double) {
      packDouble(value);
    } else if (value is ByteData) {
      packBinary(value);
    } else {
      throw new Exception("Failed to pack value: ${value}");
    }
  }

  void packAll(values) {
    for (var value in values) {
      pack(value);
    }
  }

  List<int> packBinary(ByteData bytes) {
    var count = bytes.elementSizeInBytes * bytes.lengthInBytes;

    if (count <= 255) {
      var out = new ByteData(count + 2);
      out.setUint8(0, 0xc4);
      out.setUint8(1, count);
      var i = 2;
      for (var b in bytes.buffer.asUint8List()) {
        out.setUint8(i, b);
        i++;
      }
      return out.buffer.asUint8List();
    } else if (count <= 65535) {
      var out = new ByteData(count + 3);
      out.setUint8(0, 0xc5);
      out.setUint16(1, count);
      var i = 2;
      for (var b in bytes.buffer.asUint8List()) {
        out.setUint8(i, b);
        i++;
      }
      return out.buffer.asUint8List();
    } else {
      var out = new ByteData(count + 5);
      out.setUint8(0, 0xc5);
      out.setUint32(1, count);
      var i = 2;
      for (var b in bytes.buffer.asUint8List()) {
        out.setUint8(i, b);
        i++;
      }
      return out.buffer.asUint8List();
    }
  }

  void packInt(int value) {
    if (value < 128) {
      bytes.add(value);
      return;
    }

    if (value < 0) {
      if (value >= -32) {
        bytes.add(0xe0 + value + 32);
      } else if (value > -0x80) {
        bytes.add(0xd0);
        bytes.add(value + 0x100);
      } else if (value > -0x8000) {
        bytes.add(0xd1);
        _encodeUint16(value + 0x10000);
      } else if (value > -0x80000000) {
        bytes.add(0xd2);
        _encodeUint32(value + 0x100000000);
      } else {
        bytes.add(0xd3);
        _encodeUint64(value);
      }
    } else {
      if (value < 0x100) {
        bytes.add(0xcc);
        bytes.add(value);
      } else if (value < 0x10000) {
        bytes.add(0xcd);
        _encodeUint16(value);
      } else if (value < 0x100000000) {
        bytes.add(0xce);
        _encodeUint32(value);
      } else {
        bytes.add(0xcf);
        _encodeUint64(value);
      }
    }
  }

  void _encodeUint16(int value) {
    bytes.add((value >> 8) & 0xff);
    bytes.add(value & 0xff);
  }

  void _encodeUint32(int value) {
    bytes.add((value >> 24) & 0xff);
    bytes.add((value >> 16) & 0xff);
    bytes.add((value >> 8) & 0xff);
    bytes.add(value & 0xff);
  }

  void _encodeUint64(int value) {
    bytes.add((value >> 56) & 0xff);
    bytes.add((value >> 48) & 0xff);
    bytes.add((value >> 40) & 0xff);
    bytes.add((value >> 32) & 0xff);
    bytes.add((value >> 24) & 0xff);
    bytes.add((value >> 16) & 0xff);
    bytes.add((value >> 8) & 0xff);
    bytes.add(value & 0xff);
  }

  static const Utf8Encoder _utf8Encoder = const Utf8Encoder();

  void packString(String value) {
    List<int> utf8 = _utf8Encoder.convert(value);
    if (utf8.length < 0x20) {
      bytes.add(0xa0 + utf8.length);
    } else if (utf8.length < 0x100) {
      bytes.add(0xd9);
      bytes.add(utf8.length);
    } else if (utf8.length < 0x10000) {
      bytes.add(0xda);
      _encodeUint16(utf8.length);
    } else {
      bytes.add(0xdb);
      _encodeUint32(utf8.length);
    }
    bytes.addAll(utf8);
  }

  void packDouble(double value) {
    var f = new ByteData(9);
    f.setUint8(0, 0xcb);
    f.setFloat64(1, value);
    bytes.addAll(f.buffer.asUint8List());
  }

  void packList(List value) {
    if (useStandardList) {
      if (value.length < 16) {
        bytes.add(0x90 + value.length);
      } else if (value.length < 0x100) {
        bytes.add(0xdc);
        _encodeUint16(value.length);
      } else {
        bytes.add(0xdd);
        _encodeUint32(value.length);
      }
    } else {
      bytes.add(0xdd);
      _encodeUint32(value.length);
    }

    for (var element in value) {
      pack(element);
    }
  }

  void packMap(Map value) {
    if (value.length < 16) {
      bytes.add(0x80 + value.length);
    } else if (value.length < 0x100) {
      bytes.add(0xde);
      _encodeUint16(value.length);
    } else {
      bytes.add(0xdf);
      _encodeUint32(value.length);
    }

    for (var element in value.keys) {
      pack(element);
      pack(value[element]);
    }
  }
}

class AsyncUnpacker {
  final ByteStream stream;

  AsyncUnpacker(this.stream);

  Future<int> readByte() => stream.read();

  Future unpack() async {
    int type = await readByte();

    if (type >= 0xe0) return type - 0x100;
    if (type < 0xc0) {
      if (type < 0x80) return type;
      else if (type < 0x90) return unpackMap(() async => type - 0x80);
      else if (type < 0xa0) return unpackList(() async => type - 0x90);
      else return unpackString(() async => type - 0xa0);
    }

    switch (type) {
      case 0xc0:
        return null;
      case 0xc2:
        return false;
      case 0xc3:
        return true;

      case 0xc4:
        return unpackBinary(type);
      case 0xc5:
        return unpackBinary(type);
      case 0xc6:
        return unpackBinary(type);

      case 0xcf:
        return unpackU64();
      case 0xce:
        return unpackU32();
      case 0xcd:
        return unpackU16();
      case 0xcc:
        return unpackU8();

      case 0xd3:
        return unpackS64();
      case 0xd2:
        return unpackS32();
      case 0xd1:
        return unpackS16();
      case 0xd0:
        return unpackS8();

      case 0xd9:
        return unpackString(unpackU8);
      case 0xda:
        return unpackString(unpackU16);
      case 0xdb:
        return unpackString(unpackU32);

      case 0xdf:
        return unpackMap(unpackU32);
      case 0xde:
        return unpackMap(unpackU16);
      case 0x80:
        return unpackMap(unpackU8);

      case 0xdd:
        return unpackList(unpackU32);
      case 0xdc:
        return unpackList(unpackU16);
      case 0x90:
        return unpackList(unpackU8);

      case 0xca:
        return unpackFloat32();
      case 0xcb:
        return unpackDouble();
    }
  }

  Future<ByteData> unpackBinary(int type) async {
    int count;

    if (type == 0xc4) {
      count = await readByte();
    } else if (type == 0xc5) {
      count = await stream.readUint16();
    } else if (type == 0xc6) {
      count = await stream.readUint32();
    } else {
      throw new Exception("Bad Binary Type");
    }

    return stream.readByteData(count);
  }

  Future<double> unpackFloat32() => stream.readFloat32();

  Future<double> unpackDouble() => stream.readFloat64();

  Future<int> unpackU8() => stream.read();
  Future<int> unpackU16() => stream.readUint16();
  Future<int> unpackU32() => stream.readUint32();
  Future<int> unpackU64() => stream.readUint64();

  Future<int> unpackS8() => stream.readInt8();
  Future<int> unpackS16() => stream.readInt16();
  Future<int> unpackS32() => stream.readInt32();
  Future<int> unpackS64() => stream.readInt64();

  Future<String> unpackString(Future<int> counter()) async {
    int count = await counter();
    Uint8List list = await stream.readBytes(count);
    String value = const Utf8Decoder().convert(list);
    return value;
  }

  Future<Map> unpackMap(Future<int> counter()) async {
    var count = await counter();
    Map map = {};
    for (int i = 0; i < count; ++i) {
      map[await unpack()] = await unpack();
    }
    return map;
  }

  Future<List> unpackList(Future<int> counter()) async {
    var count = await counter();
    List list = [];
    list.length = count;
    for (int i = 0; i < count; ++i) {
      list[i] = await unpack();
    }
    return list;
  }
}
