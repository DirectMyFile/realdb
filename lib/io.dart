library realdb.io;

import "dart:async";
import "dart:io";
import "dart:typed_data";

abstract class ByteStream {
  Future<int> read();
  Future<int> getPosition();

  Future skip(int count) async {
    for (var i = 1; i <= count; i++) {
      await read();
    }
  }

  Future<Uint8List> readBytes(int count) async {
    var list = new Uint8List(count);
    for (var i = 0; i < count; i++) {
      list[i] = await read();
    }
    return list;
  }

  Future<ByteData> readByteData(int count) async {
    var data = new ByteData(count);
    for (var i = 0; i < count; i++) {
      data.setUint8(i, await read());
    }
    return data;
  }

  Future<int> readUint16() async {
    return (await readByteData(2)).getUint16(0);
  }

  Future<int> readUint32() async {
    return (await readByteData(4)).getUint32(0);
  }

  Future<int> readUint64() async {
    return (await readByteData(8)).getUint64(0);
  }

  Future<int> readInt8() async {
    return (await readByteData(1)).getInt8(0);
  }

  Future<int> readInt16() async {
    return (await readByteData(2)).getInt16(0);
  }

  Future<int> readInt32() async {
    return (await readByteData(4)).getInt32(0);
  }

  Future<int> readInt64() async {
    return (await readByteData(8)).getInt64(0);
  }

  Future<double> readFloat32() async {
    return (await readByteData(4)).getFloat32(0);
  }

  Future<double> readFloat64() async {
    return (await readByteData(8)).getFloat64(0);
  }

  Future close();
}

class ByteDataStream extends ByteStream {
  final ByteData _data;

  int _pos = -1;

  ByteDataStream(this._data);

  @override
  Future<int> getPosition() async {
    return _pos;
  }

  @override
  Future<int> read() async {
    _pos++;
    return _data.getUint8(_pos);
  }

  @override
  Future close() async {}
}

class FileByteStream extends ByteStream {
  final RandomAccessFile _file;

  FileByteStream(this._file);

  @override
  Future<int> read() => _file.readByte();

  @override
  Future<int> getPosition() => _file.position();

  @override
  Future skip(int count) async =>
    _file.setPosition((await _file.position()) + count);

  @override
  Future close() async => await _file.close();
}

class ChunkedFileByteStream extends FileByteStream {
  final int chunkSize;

  int _pos = -1;
  int _buffPos = -1;
  Uint8List _buff;

  ChunkedFileByteStream(this.chunkSize, RandomAccessFile file) : super(file) {
    _buff = new Uint8List(chunkSize);
  }

  @override
  Future<int> read() => _read();

  Future<int> _read({bool fake: false}) async {
    if (_buffPos == -1 || _buffPos == chunkSize - 1) {
      await _file.readInto(_buff);
      _buffPos = -1;
    }

    _pos++;
    _buffPos++;

    if (!fake) {
      var b = _buff[_buffPos];
      return b;
    } else {
      return null;
    }
  }

  @override
  Future<int> getPosition() async {
    return _pos;
  }

  @override
  Future skip(int count) async {
    if (_buffPos + count < chunkSize) {
      if (_buffPos == -1 || _buffPos == chunkSize - 1) {
        await _file.readInto(_buff);
        _buffPos = -1;
      }

      _buffPos += count;
    } else {
      for (var i = 1; i <= count; i++) {
        await _read(fake: true);
      }
    }
  }
}
