package org.folio.inventoryupdate.referencemapping;

import java.util.Set;
import java.util.TreeSet;

public class AlternateFKValues {

  ReferenceApi api;
  Set<String> alternateIds = new TreeSet<>();

  public AlternateFKValues(ReferenceApi refApi, Set<String> alternateIds) {
    this.api = refApi;
    this.alternateIds = alternateIds;
  }

  public AlternateFKValues(ReferenceApi list, String alternateId) {
    this.api = list;
    this.alternateIds.add(alternateId);
  }

  public void addAlternateIds(Set<String> alternateIds) {
    this.alternateIds.addAll(alternateIds);
  }

  public Set<String> getAlternateIds() {
    return alternateIds;
  }

  public ReferenceApi getReferenceApi () {
    return api;
  }

}
