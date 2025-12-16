package org.folio.inventoryupdate.importing.utils;

import javax.xml.transform.ErrorListener;
import javax.xml.transform.TransformerException;
import java.util.ArrayList;
import java.util.List;

public class XsltParsingErrors implements ErrorListener {
  final List<String> issues = new ArrayList<>();
  @Override
  public void warning(TransformerException e) {
    issues.add("Warning. Line " + (e.getLocator() != null ? e.getLocator().getLineNumber() : "") + ": " + e.getMessage());
  }

  @Override
  public void error(TransformerException e)  {
    issues.add("Error. Line " + (e.getLocator() != null ? e.getLocator().getLineNumber() : "") + ": " + e.getMessage());
  }

  @Override
  public void fatalError(TransformerException e) throws TransformerException {
    issues.add("FatalError. Line " + (e.getLocator() != null ? e.getLocator().getLineNumber() : "") + ": " + e.getMessage());
    throw e;
  }

  public String toString() {
    StringBuilder errors = new StringBuilder();
    for (String issue : issues) {
      errors.append(System.lineSeparator()).append(issue);
    }
    return errors.toString();
  }

}
