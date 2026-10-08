package org.apache.hadoop.explorer.yarn.service;

import org.apache.hadoop.explorer.yarn.model.ClusterConfig;
import org.apache.hadoop.explorer.yarn.model.PartitionResourceConfig;
import org.apache.hadoop.explorer.yarn.model.QueueDraftItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.ByteArrayInputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Service
public class XmlGeneratorService {

    private static final Logger log = LoggerFactory.getLogger(XmlGeneratorService.class);

    private static final Set<String> MANAGED_QUEUE_SUFFIXES = Set.of(
            "queues", "capacity", "maximum-capacity", "state", "user-limit-factor",
            "ordering-policy", "maximum-applications", "max-applications",
            "maximum-am-resource-percent", "max-parallel-apps",
            "maximum-application-lifetime", "accessible-node-labels",
            "default-node-label-expression"
    );

    private static final Set<String> GLOBAL_MANAGED_PROPERTIES = Set.of(
            "yarn.scheduler.capacity.queue-mappings",
            "yarn.scheduler.capacity.queue-mappings-override.enable"
    );

    /**
     * Генерирует или обновляет XML конфигурацию capacity-scheduler.xml.
     */
    public String generateCapacitySchedulerXml(
            List<QueueDraftItem> queues,
            ClusterConfig cluster,
            String generatedBy,
            String comment,
            String resourceMode,
            String queueMappings,
            Boolean queueMappingsOverride,
            String baseXml
    ) {
        try {
            // Вычисляем словарь управляемых свойств
            Map<String, String> managedProperties = computeManagedProperties(queues, cluster, queueMappings, queueMappingsOverride);

            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            dbf.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            dbf.setFeature("http://xml.org/sax/features/external-general-entities", false);
            dbf.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
            DocumentBuilder db = dbf.newDocumentBuilder();

            Document doc;
            if (baseXml != null && !baseXml.isBlank()) {
                doc = db.parse(new ByteArrayInputStream(baseXml.getBytes(StandardCharsets.UTF_8)));
            } else {
                doc = db.newDocument();
                Element configuration = doc.createElement("configuration");
                doc.appendChild(configuration);
            }

            Element root = doc.getDocumentElement();

            // Удаляем существующие управляемые свойства
            NodeList children = root.getChildNodes();
            List<Node> toRemove = new ArrayList<>();
            for (int i = 0; i < children.getLength(); i++) {
                Node node = children.item(i);
                if (node.getNodeType() == Node.ELEMENT_NODE && "property".equals(node.getNodeName())) {
                    Element propElem = (Element) node;
                    NodeList nameNodes = propElem.getElementsByTagName("name");
                    if (nameNodes.getLength() > 0) {
                        String propName = nameNodes.item(0).getTextContent().trim();
                        if (isManagedProperty(propName, queues)) {
                            toRemove.add(node);
                        }
                    }
                }
            }
            for (Node n : toRemove) {
                root.removeChild(n);
            }

            // Добавляем новые управляемые свойства в отсортированном порядке
            List<String> sortedKeys = new ArrayList<>(managedProperties.keySet());
            Collections.sort(sortedKeys);

            for (String propName : sortedKeys) {
                String val = managedProperties.get(propName);
                if (val == null) continue;

                Element propElem = doc.createElement("property");
                Element nameElem = doc.createElement("name");
                nameElem.setTextContent(propName);
                Element valElem = doc.createElement("value");
                valElem.setTextContent(val);

                propElem.appendChild(nameElem);
                propElem.appendChild(valElem);
                root.appendChild(propElem);
            }

            // Формируем заголовочный комментарий
            String timestamp = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss 'UTC'")
                    .withZone(ZoneOffset.UTC)
                    .format(Instant.now());

            String commentText = String.format("\n" +
                    "  Generated by Hadoop Explorer YARN Module\n" +
                    "  Timestamp: %s\n" +
                    "  Author: %s\n" +
                    "  Description: %s\n",
                    timestamp,
                    generatedBy != null ? generatedBy : "system",
                    comment != null ? comment.replace("--", "- -") : ""
            );

            doc.insertBefore(doc.createComment(commentText), root);

            // Преобразуем DOM в строку XML
            TransformerFactory tf = TransformerFactory.newInstance();
            Transformer transformer = tf.newTransformer();
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");
            transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
            transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");

            StringWriter writer = new StringWriter();
            transformer.transform(new DOMSource(doc), new StreamResult(writer));
            return writer.toString();

        } catch (Exception e) {
            log.error("Ошибка при генерации capacity-scheduler.xml: {}", e.getMessage(), e);
            throw new RuntimeException("Не удалось сгенерировать capacity-scheduler.xml: " + e.getMessage(), e);
        }
    }

    private boolean isManagedProperty(String propName, List<QueueDraftItem> queues) {
        if (GLOBAL_MANAGED_PROPERTIES.contains(propName)) {
            return true;
        }
        String prefix = "yarn.scheduler.capacity.";
        if (!propName.startsWith(prefix)) {
            return false;
        }
        String remainder = propName.substring(prefix.length());

        for (String suffix : MANAGED_QUEUE_SUFFIXES) {
            if (remainder.endsWith("." + suffix) || remainder.equals(suffix)) {
                return true;
            }
        }
        return false;
    }

    private Map<String, String> computeManagedProperties(
            List<QueueDraftItem> queues,
            ClusterConfig cluster,
            String queueMappings,
            Boolean queueMappingsOverride
    ) {
        Map<String, String> props = new LinkedHashMap<>();

        // 1. Построение списка дочерних очередей для каждого родителя
        Map<String, List<String>> childrenMap = new LinkedHashMap<>();
        for (QueueDraftItem q : queues) {
            if ("delete".equalsIgnoreCase(q.getAction())) continue;
            String parent = q.getParentPath() != null ? q.getParentPath() : "root";
            if (!q.getPath().equals("root")) {
                childrenMap.computeIfAbsent(parent, k -> new ArrayList<>()).add(q.getName());
            }
        }

        // Записываем yarn.scheduler.capacity.<parent>.queues
        for (Map.Entry<String, List<String>> entry : childrenMap.entrySet()) {
            String parent = entry.getKey();
            List<String> childNames = entry.getValue();
            if (!childNames.isEmpty()) {
                props.put("yarn.scheduler.capacity." + parent + ".queues", String.join(",", childNames));
            }
        }

        // 2. Свойства для каждой очереди
        for (QueueDraftItem q : queues) {
            if ("delete".equalsIgnoreCase(q.getAction())) continue;

            String path = q.getPath();

            // Partitions (capacity & maximum-capacity)
            if (q.getPartitions() != null) {
                for (Map.Entry<String, PartitionResourceConfig> pEntry : q.getPartitions().entrySet()) {
                    String partName = pEntry.getKey();
                    PartitionResourceConfig pCfg = pEntry.getValue();

                    String capProp;
                    String maxCapProp;
                    if ("DEFAULT".equalsIgnoreCase(partName)) {
                        capProp = "yarn.scheduler.capacity." + path + ".capacity";
                        maxCapProp = "yarn.scheduler.capacity." + path + ".maximum-capacity";
                    } else {
                        capProp = "yarn.scheduler.capacity." + path + "." + partName + ".capacity";
                        maxCapProp = "yarn.scheduler.capacity." + path + "." + partName + ".maximum-capacity";
                    }

                    props.put(capProp, String.valueOf(pCfg.getCapacity()));
                    props.put(maxCapProp, String.valueOf(pCfg.getMaxCapacity()));
                }
            }

            if (q.getState() != null) {
                props.put("yarn.scheduler.capacity." + path + ".state", q.getState().name());
            }

            if (q.getUserLimitFactor() != null) {
                props.put("yarn.scheduler.capacity." + path + ".user-limit-factor", String.valueOf(q.getUserLimitFactor()));
            }

            if (q.getOrderingPolicy() != null && !q.getOrderingPolicy().isBlank()) {
                props.put("yarn.scheduler.capacity." + path + ".ordering-policy", q.getOrderingPolicy().toLowerCase());
            }

            if (q.getMaxApplications() != null) {
                props.put("yarn.scheduler.capacity." + path + ".maximum-applications", String.valueOf(q.getMaxApplications()));
            }

            if (q.getMaxAmResourcePercent() != null) {
                props.put("yarn.scheduler.capacity." + path + ".maximum-am-resource-percent", String.valueOf(q.getMaxAmResourcePercent()));
            }

            if (q.getAccessibleNodeLabels() != null && !q.getAccessibleNodeLabels().isEmpty()) {
                props.put("yarn.scheduler.capacity." + path + ".accessible-node-labels", String.join(",", q.getAccessibleNodeLabels()));
            }

            if (q.getDefaultNodeLabelExpression() != null && !q.getDefaultNodeLabelExpression().isBlank()) {
                props.put("yarn.scheduler.capacity." + path + ".default-node-label-expression", q.getDefaultNodeLabelExpression());
            }
        }

        // 3. Queue Mappings
        String mappings = (queueMappings != null) ? queueMappings : cluster.getQueueMappings();
        if (mappings != null && !mappings.isBlank()) {
            props.put("yarn.scheduler.capacity.queue-mappings", mappings);
        }

        boolean override = (queueMappingsOverride != null) ? queueMappingsOverride : cluster.isQueueMappingsOverride();
        props.put("yarn.scheduler.capacity.queue-mappings-override.enable", String.valueOf(override));

        return props;
    }
}
