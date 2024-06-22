package org.folio.inventoryupdate.referencemapping;

public record ForeignKey(String foreignKeyName, String foreignKeyEmbeddedIn, ReferenceApi referencedApi) {}
