package hu.sztaki.streaming.example.cellinfo.util;

public class Util {
  public static int mod(int x, int y) {
    int result = x % y;
    if (result < 0) {
      result += y;
    }
    return result;
  }
}
