package org.folio.inventorymatch.test;

public class HridSequencer {

  public static HridSequencer sequencer = null;

  private HridSequencer() {
  }

  public static HridSequencer getSequencer() {
    if (sequencer == null) {
      sequencer = new HridSequencer();
    }
    return sequencer;
  }
}
