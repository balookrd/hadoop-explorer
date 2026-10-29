package org.apache.hadoop.explorer.yarn.service;

import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilderFactory;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Sanity checks for a complete capacity-scheduler.xml uploaded for direct deployment.
 */
public final class CapacitySchedulerXmlValidator {

    private CapacitySchedulerXmlValidator() {
    }

    public static void validate(String xml) {
        if (xml == null || xml.isBlank()) {
            throw bad("Пустой XML");
        }
        Document doc;
        try {
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            dbf.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            dbf.setFeature("http://xml.org/sax/features/external-general-entities", false);
            dbf.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
            doc = dbf.newDocumentBuilder().parse(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) {
            throw bad("Некорректный XML: " + e.getMessage());
        }

        Element root = doc.getDocumentElement();
        if (!"configuration".equals(root.getNodeName())) {
            throw bad("Корневой элемент должен быть <configuration>, найден <" + root.getNodeName() + ">");
        }

        List<String> foreign = new ArrayList<>();
        boolean hasRootQueues = false;
        NodeList props = root.getChildNodes();
        for (int i = 0; i < props.getLength(); i++) {
            Node n = props.item(i);
            if (n.getNodeType() != Node.ELEMENT_NODE || !"property".equals(n.getNodeName())) {
                continue;
            }
            NodeList names = ((Element) n).getElementsByTagName("name");
            String name = names.getLength() > 0 ? names.item(0).getTextContent().trim() : "";
            if (!name.startsWith("yarn.scheduler.capacity.")) {
                foreign.add(name.isEmpty() ? "<без имени>" : name);
            }
            if ("yarn.scheduler.capacity.root.queues".equals(name)) {
                hasRootQueues = true;
            }
        }
        if (!foreign.isEmpty()) {
            throw bad("В capacity-scheduler.xml допустимы только свойства yarn.scheduler.capacity.*; лишние: "
                    + String.join(", ", foreign.subList(0, Math.min(10, foreign.size())))
                    + (foreign.size() > 10 ? " и еще " + (foreign.size() - 10) : ""));
        }
        if (!hasRootQueues) {
            throw bad("Не задано свойство yarn.scheduler.capacity.root.queues");
        }
    }

    private static ResponseStatusException bad(String message) {
        return new ResponseStatusException(HttpStatus.BAD_REQUEST, message);
    }
}
