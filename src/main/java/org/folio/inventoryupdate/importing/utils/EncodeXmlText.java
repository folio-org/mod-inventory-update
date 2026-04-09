package org.folio.inventoryupdate.importing.utils;

public final class EncodeXmlText {

  private EncodeXmlText() {
  }

  private static final String REPLACEMENT_CHAR = "&#xFFFD;";

  /**
   * Encode encode XML string.
   *
   * @param s string
   * @return encoded string
   */
  public static String encodeXmlText(String s) {
    StringBuilder sb = new StringBuilder();
    int len = s.length();
    for (int i = 0; i < len; ) {
      int c = s.codePointAt(i);
      if (c < 0x80) { // ASCII
        if (c < 0x20 && (c != '\t' && c != '\r' && c != '\n')) {
          // illegal XML character even if escaped, substitute
          sb.append(REPLACEMENT_CHAR);
        } else {
          switch (c) {
            case '&':
              sb.append("&amp;");
              break;
            case '>':
              sb.append("&gt;");
              break;
            case '<':
              sb.append("&lt;");
              break;
            //only needed for XML attribute but we escape always
            case '\'':
              sb.append("&apos;");
              break;
            case '\"':
              sb.append("&quot;");
              break;
            default:
              sb.append((char) c);
          }
        }
      } else if ((c >= 0xd800 && c <= 0xdfff) || c == 0xfffe || c == 0xffff) {
        // illegal XML character even if escaped, replace
        sb.append(REPLACEMENT_CHAR);
      } else {
        //legal unicode if escaped
        sb.append("&#x");
        sb.append(Integer.toHexString(c));
        sb.append(';');
      }
      i += c <= 0xffff ? 1 : 2;
    }
    return sb.toString();
  }
}

