package org.folio.inventoryupdate.referencemapping;


  public enum ReferenceApi {
    ALTERNATIVE_TITLE_TYPES("/alternative-title-types", "alternativeTitleTypes", "name"),
    CALL_NUMBER_TYPES("/call-number-types", "callNumberTypes", "name"),
    CLASSIFICATION_TYPES("/classification-types", "classificationTypes", "name"),
    CONTRIBUTOR_NAME_TYPES("/contributor-name-types", "contributorNameTypes", "name"),
    CONTRIBUTOR_TYPES("/contributor-types", "contributorTypes", "code"),
    ELECTRONIC_ACCESS_RELATIONSHIPS("/electronic-access-relationships", "electronicAccessRelationships", "name"),
    HOLDINGS_NOTE_TYPES("/holdings-note-types", "holdingsNoteTypes", "name"),
    HOLDINGS_SOURCES("/holdings-sources", "holdingsRecordsSources", "name"),
    HOLDINGS_TYPES("/holdings-types", "holdingsTypes", "name"),
    IDENTIFIER_TYPES("/identifier-types", "identifierTypes", "name"),
    ILL_POLICIES("/ill-policies", "illPolicies", "name"),
    INSTANCE_FORMATS("/instance-formats", "instanceFormats", "code"),
    INSTANCE_NOTE_TYPES("/instance-note-types", "instanceNoteTypes", "name"),
    INSTANCE_STATUSES("/instance-statuses", "instanceStatuses", "code"),
    INSTANCE_TYPES("/instance-types", "instanceTypes", "code"),
    ITEM_DAMAGED_STATUSES("/item-damaged-statuses", "itemDamageStatuses", "name"),
    ITEM_NOTE_TYPES("/item-note-types", "itemNoteTypes", "name"),
    LOAN_TYPES("/loan-types", "loantypes", "name"),
    MATERIAL_TYPES("/material-types", "mtypes", "name"),
    MODES_OF_ISSUANCE("/modes-of-issuance", "issuanceModes", "name"),
    NATURE_OF_CONTENT_TERMS("/nature-of-content-terms", "natureOfContentTerms", "name"),
    STATISTICAL_CODES("/statistical-codes", "statisticalCodes", "code");

    //INSTANCE_RELATIONSHIP_TYPES("/instance-relationship-types", "instanceRelationshipTypes", ),

    private final String path;
    private final String recordsArray;
    private final String alternateKey;

    ReferenceApi(String path, String recordsArray, String alternateKey) {
      this.path = path;
      this.recordsArray = recordsArray;
      this.alternateKey = alternateKey;
    }

    public String getPath() {
      return path;
    }

    public String getArrayName() {
      return recordsArray;
    }

    public String getAlternateKey() {
      return alternateKey;
    }
  }
