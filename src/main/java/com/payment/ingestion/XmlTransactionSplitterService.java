package com.payment.ingestion;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.BufferedInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Service
public class XmlTransactionSplitterService {

    @Value("${app.processing.output-dir:./data/output}")
    private String outputDir;

    public List<TransactionMetadata> splitAndExtractMetadata(Path inputFilePath, String jobId) throws IOException, XMLStreamException {
        List<TransactionMetadata> metadataList = new ArrayList<>();
        XMLInputFactory factory = XMLInputFactory.newInstance();

        // Prevent XML External Entity (XXE) vulnerabilities
        factory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, Boolean.FALSE);
        factory.setProperty(XMLInputFactory.SUPPORT_DTD, Boolean.FALSE);

        Path outputDirectory = Paths.get(outputDir);
        if (!Files.exists(outputDirectory)) {
            Files.createDirectories(outputDirectory);
        }

        String msgId = "UNKNOWN";

        try (InputStream fis = new BufferedInputStream(Files.newInputStream(inputFilePath))) {
            XMLStreamReader reader = factory.createXMLStreamReader(fis);

            while (reader.hasNext()) {
                int event = reader.next();

                if (event == XMLStreamConstants.START_ELEMENT) {
                    String localName = reader.getLocalName();

                    if ("MsgId".equalsIgnoreCase(localName)) {
                        msgId = reader.getElementText();
                    } else if ("CdtTrfTxInf".equalsIgnoreCase(localName)) {
                        TransactionMetadata metadata = processSingleTransaction(reader, jobId, msgId, outputDirectory);
                        metadataList.add(metadata);
                    }
                }
            }
            reader.close();
        }

        return metadataList;
    }

    private TransactionMetadata processSingleTransaction(XMLStreamReader reader, String jobId, String msgId, Path outputDirectory)
            throws XMLStreamException, IOException {

        StringBuilder xmlBuffer = new StringBuilder();
        xmlBuffer.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
        xmlBuffer.append("<Document xmlns=\"urn:iso:std:iso:20022:tech:xsd:pacs.009.001.08\">\n");

        String instructionId = null;
        String endToEndId = null;
        String transactionId = null;
        String uetr = null;
        BigDecimal amount = null;
        String currency = null;
        LocalDate settlementDate = null;
        String debtorBic = null;
        String creditorBic = null;

        boolean insideDbtr = false;
        boolean insideCdtr = false;
        int depth = 1;

        xmlBuffer.append("  <CdtTrfTxInf>\n");

        while (reader.hasNext() && depth > 0) {
            int event = reader.next();

            switch (event) {
                case XMLStreamConstants.START_ELEMENT:
                    depth++;
                    String elementName = reader.getLocalName();
                    xmlBuffer.append("    ".repeat(depth)).append("<").append(elementName);

                    for (int i = 0; i < reader.getAttributeCount(); i++) {
                        String attrName = reader.getAttributeLocalName(i);
                        String attrVal = reader.getAttributeValue(i);
                        xmlBuffer.append(" ").append(attrName).append("=\"").append(attrVal).append("\"");

                        if ("IntrBkSttlmAmt".equalsIgnoreCase(elementName) && "Ccy".equalsIgnoreCase(attrName)) {
                            currency = attrVal;
                        }
                    }
                    xmlBuffer.append(">");

                    if ("Dbtr".equalsIgnoreCase(elementName)) insideDbtr = true;
                    if ("Cdtr".equalsIgnoreCase(elementName)) insideCdtr = true;
                    break;

                case XMLStreamConstants.CHARACTERS:
                    String text = reader.getText().trim();
                    if (!text.isEmpty()) {
                        xmlBuffer.append(text);
                    }
                    break;

                case XMLStreamConstants.END_ELEMENT:
                    String endName = reader.getLocalName();
                    xmlBuffer.append("</").append(endName).append(">\n");

                    if ("InstructionId".equalsIgnoreCase(endName)) instructionId = reader.getText(); // Captured during element read
                    if ("EndToEndId".equalsIgnoreCase(endName)) endToEndId = reader.getText();
                    if ("TxId".equalsIgnoreCase(endName)) transactionId = reader.getText();
                    if ("UETR".equalsIgnoreCase(endName)) uetr = reader.getText();
                    if ("IntrBkSttlmDt".equalsIgnoreCase(endName)) {
                        try {
                            settlementDate = LocalDate.parse(reader.getText().trim());
                        } catch (Exception ignored) {}
                    }
                    if ("Dbtr".equalsIgnoreCase(endName)) insideDbtr = false;
                    if ("Cdtr".equalsIgnoreCase(endName)) insideCdtr = false;

                    depth--;
                    break;
            }
        }

        xmlBuffer.append("</Document>");

        // Fallback for missing UETR
        if (uetr == null) {
            uetr = UUID.randomUUID().toString();
        }

        String outputFileName = "pacs009_" + uetr + ".xml";
        Path outputPath = outputDirectory.resolve(outputFileName);

        try (FileWriter writer = new FileWriter(outputPath.toFile())) {
            writer.write(xmlBuffer.toString());
        }

        return TransactionMetadata.builder()
                .id(UUID.randomUUID().toString())
                .jobId(jobId)
                .msgId(msgId)
                .instructionId(instructionId)
                .endToEndId(endToEndId)
                .transactionId(transactionId != null ? transactionId : "TXN-" + uetr.substring(0, 8))
                .uetr(uetr)
                .amount(amount != null ? amount : new BigDecimal("0.00"))
                .currency(currency != null ? currency : "USD")
                .settlementDate(settlementDate != null ? settlementDate : LocalDate.now())
                .debtorBic(debtorBic)
                .creditorBic(creditorBic)
                .chunkedFilePath(outputPath.toAbsolutePath().toString())
                .createdAt(Instant.now())
                .build();
    }
}