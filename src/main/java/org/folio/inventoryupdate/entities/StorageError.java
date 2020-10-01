package org.folio.inventoryupdate.entities;

public class StorageError {

        private int statusCode;
        private String reason;
        private String response;
        private String shortDescription;
        private String entity;

        public int getStatusCode() {
            return statusCode;
        }

        public String getReason() {
            return reason;
        }

        public String getResponse() {
            return response;
        }

        public String getShortDescription() {
            return shortDescription;
        }

        public String getEntity() {
            return entity;
        }


        public StorageError(int status, String reason, String response, String shortDescription, String entity) {
          this.statusCode = status;
          this.reason = reason;
          this.response = response;
          this.shortDescription = shortDescription;
          this.entity = entity;
        }

        public StorageError(int status, String reason, String response, String shortDescription) {
          this(status, reason, response, shortDescription, "unspecified");
        }

        @Override
        public String toString() {
          return entity + ": " + shortDescription + ". Status code ["+statusCode+"]." + reason + "]." + response;
        }

        public String getMessage() {
          return shortDescription + "; " + reason + "; " + response;
        }

        public String getLabel() {
          return shortDescription;
        }

        public String getType() {
          return reason;
        }

        public String getBriefMessage() {
          return response;
        }

        public String getStorageEntity() {
          return entity;
        }

}