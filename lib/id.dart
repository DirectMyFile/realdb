library realdb.id;

import "dart:async";
import "dart:math";

Random random = new Random();

Future<String> generateBasicId({int length: 30}) async {
  var r0 = new Random();
  var buffer = new StringBuffer();
  for (int i = 1; i <= length; i++) {
    var r = new Random(r0.nextInt(0x70000000) + (new DateTime.now()).millisecondsSinceEpoch);
    await new Future.value();
    var n = r.nextInt(50);
    if (n >= 0 && n <= 32) {
      String letter = alphabet[r.nextInt(alphabet.length)];
      buffer.write(r.nextBool() ? letter.toLowerCase() : letter);
    } else if (n > 32 && n <= 43) {
      buffer.write(numbers[r.nextInt(numbers.length)]);
    } else if (n > 43) {
      buffer.write(specials[r.nextInt(specials.length)]);
    }
  }
  return buffer.toString();
}

String generateToken({int length: 50}) {
  var r = new Random();
  var buffer = new StringBuffer();
  for (int i = 1; i <= length; i++) {
    if (r.nextBool()) {
      String letter = alphabet[r.nextInt(alphabet.length)];
      buffer.write(r.nextBool() ? letter.toLowerCase() : letter);
    } else {
      buffer.write(numbers[r.nextInt(numbers.length)]);
    }
  }
  return buffer.toString();
}

Future<String> generateStrongToken({int length: 50}) async {
  var r0 = new Random();
  var buffer = new StringBuffer();
  for (int i = 1; i <= length; i++) {
    await new Future.value();
    var r = new Random(r0.nextInt(0x70000000) + (new DateTime.now()).millisecondsSinceEpoch);
    if (r.nextBool()) {
      String letter = alphabet[r.nextInt(alphabet.length)];
      buffer.write(r.nextBool() ? letter.toLowerCase() : letter);
    } else {
      buffer.write(numbers[r.nextInt(numbers.length)]);
    }
  }
  return buffer.toString();
}

const List<String> alphabet = const [
  "A",
  "B",
  "C",
  "D",
  "E",
  "F",
  "G",
  "H",
  "I",
  "J",
  "K",
  "L",
  "M",
  "N",
  "O",
  "P",
  "Q",
  "R",
  "S",
  "T",
  "U",
  "V",
  "W",
  "X",
  "Y",
  "Z"
];

const List<int> numbers = const [
  0,
  1,
  2,
  3,
  4,
  5,
  6,
  7,
  8,
  9
];

const List<String> specials = const [
  "@",
  "=",
  "_",
  "+",
  "-",
  "!",
  "."
];
