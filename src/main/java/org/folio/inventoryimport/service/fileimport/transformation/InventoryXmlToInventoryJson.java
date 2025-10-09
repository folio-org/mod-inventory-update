package org.folio.inventoryimport.service.fileimport.transformation;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventoryimport.utils.SecureSaxParser;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.StringReader;
import java.util.*;

public class InventoryXmlToInventoryJson {

    public static final Logger logger = LogManager.getLogger("InventoryXmlToInventoryJson");

    public static JsonObject convert(String xmlStr)  {
        JsonObject genericJson = parseXmlToJson(xmlStr);
        return genericJson == null ? new JsonObject() : makeInventoryJson(genericJson);
    }

    public static JsonObject parseXmlToJson(String xmlStr)  {
        try {
            XMLToJSONHandler handler = new XMLToJSONHandler();
            SecureSaxParser.get().parse(new InputSource(new StringReader(xmlStr)), handler);
            return new JsonObject(handler.getData());
        } catch (ParserConfigurationException | SAXException e) {
            logger.error("Error parsing XML to JSON: " + e.getMessage());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    // SAX parser, XML-to-JSON.
    public static class XMLToJSONHandler extends DefaultHandler {
        private final Stack<Map<String, Object>> stack = new Stack<>();
        private Map<String, Object> currentData = new HashMap<>();

        @Override
        public void startElement(String uri, String localName, String qName, Attributes attributes) {
            Map<String, Object> element = new HashMap<>();
            element.put("name", qName);
            if (!stack.isEmpty()) {
                stack.peek().computeIfAbsent("children", k -> new ArrayList<Map<String, Object>>());
                ((List<Map<String, Object>>) stack.peek().get("children")).add(element);
            }
            stack.push(element);
        }

        @Override
        public void endElement(String uri, String localName, String qName) {
            if (!stack.isEmpty()) {
                currentData = stack.pop();
            }
        }

        @Override
        public void characters(char[] ch, int start, int length) {
            String content = new String(ch, start, length).trim();
            if (!content.isEmpty()) {
                content = content.replace("&", "&amp;");
                content = content.replace("<", "&lt;");
                content = content.replace(">", "&gt;");
                stack.peek().put("text", content);
            }
        }

        public Map<String, Object> getData() {
            return currentData;
        }
    }

    private static JsonObject makeInventoryJson(JsonObject genericJson) {
        JsonObject json = new JsonObject();
        if (genericJson.containsKey("children")) {
            return makeInventoryJsonObjects(json, genericJson.getJsonArray("children").getJsonObject(0));
        }
        return json;
    }

    private static JsonObject makeInventoryJsonObjects(JsonObject toJson, JsonObject genericJson) {
        String propertyName = genericJson.getString("name");
        if (genericJson.containsKey("children")) {
            JsonArray childProperties = genericJson.getJsonArray("children");
            if (childProperties.getJsonObject(0).getString("name").equals("arr")) {
                JsonArray toArray = new JsonArray();
                toJson.put(genericJson.getString("name"), toArray);
                makeInventoryJsonArray(toArray, childProperties);
            } else {
                if (propertyName.equals("record")) {
                    for (Object childProperty : childProperties) {
                        makeInventoryJsonObjects(toJson, (JsonObject) childProperty);
                    }
                } else {
                    JsonObject toProperty = new JsonObject();
                    toJson.put(propertyName, toProperty);
                    for (Object childProperty : childProperties) {
                        makeInventoryJsonObjects(toProperty, (JsonObject) childProperty);
                    }
                }
            }
        } else if (genericJson.containsKey("text")) {
            toJson.put(propertyName, genericJson.getString("text"));
        }
        return toJson;
    }

    private static void makeInventoryJsonArray(JsonArray toJsonArray, JsonArray genericJsonArray) {
        for (Object o : genericJsonArray) {
            JsonObject element = (JsonObject) o;
            if (element.containsKey("children")) {
                for (Object child : element.getJsonArray("children")) {
                    if (((JsonObject) child).containsKey("children")) {
                        // array of objects
                        JsonObject arrayElement = new JsonObject();
                        JsonArray children = ((JsonObject) child).getJsonArray("children");
                        for (Object prop : children) {
                            makeInventoryJsonObjects(arrayElement, (JsonObject) prop);
                        }
                        toJsonArray.add(arrayElement);
                    } else if (((JsonObject) child).containsKey("text")) {
                        // array of strings
                        toJsonArray.add(((JsonObject) child).getString("text"));
                    }
                }
            }
        }
    }
}



