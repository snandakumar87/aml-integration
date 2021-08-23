/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// camel-k: language=java

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaComponent;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;


public class AmlEvent extends RouteBuilder {
    private String consumerMaxPollRecords = "50000";
    private String consumerCount = "3";
    private String consumerSeekTo = "beginning";
    private String consumerGroup = "customereventhistory";
    private  String kafkaBootstrap = "my-cluster-kafka-brokers:9092";

    @Override
    public void configure() throws Exception {

        GetCardTransactions getCardTransactions = new GetCardTransactions();
        from("kafka:" + "event-input-stream" + "?brokers=" + kafkaBootstrap + "&maxPollRecords="
                + consumerMaxPollRecords + "&seekTo=" + "beginning"
                + "&groupId=" + consumerGroup)
                .process(getCardTransactions)
                .process(new GetPrediction())
                .setHeader(Exchange.CONTENT_TYPE, constant("application/json"))
                .setHeader(Exchange.ACCEPT_CONTENT_TYPE, constant("application/json"))
                .setHeader(Exchange.HTTP_METHOD, constant("POST"))
                .to("http://xgboost-default-classifier-odh-project.apps.cluster-5e7a.5e7a.sandbox445.opentlc.com/api/v1.0/predictions")
                .log("${body}")
                .convertBodyTo(String.class)
                .process(new KafkProcessor())
                .log("${body}")
                .to("kafka:"+"transaction-decision-reqest"+ "?brokers=" + kafkaBootstrap);





    }


    private final class GetCardTransactions implements Processor {

        String responseBodyStart = "{\n" +
                "\t\"specversion\": \"1.0\",\n" +
                "\t\"id\": \"a89b61a2-5644-487a-8a86-144855c5dce8\",\n" +
                "\t\"source\": \"SomeEventSource\",\n" +
                "\t\"type\": \"DecisionRequest\",\n" +
                "\t\"subject\": \"TheSubject\",\n" +
                "\t\"kogitodmnmodelname\": \"TransactionMonitoringDMN\",\n" +
                "\t\"kogitodmnmodelnamespace\": \"https://kiegroup.org/dmn/_EED47FB5-8A7C-44F3-A786-563FD2DAF015\",\n" +
                "\t\"data\": {\n" +
                "\t\t\"Transaction\": { \"transactionAmount\": ";

        @Override
        public void process(Exchange exchange) throws Exception {

                java.util.Map valueMap = new com.fasterxml.jackson.databind.ObjectMapper().readValue(exchange.getIn().getBody().toString(), java.util.HashMap.class);

                String transactionType = (String) valueMap.get("eventType");
            String paymentMode = (String) valueMap.get("paymentMode");

                Integer transactionAmont = (Integer) valueMap.get("amount");

                Long transactionId = (Long) valueMap.get("transactionId");
                exchange.getIn().setHeader("transactionAmount",transactionAmont);

            String transactionCountry = (String) valueMap.get("transactionCountry");

                System.out.println(transactionAmont+ " " + transactionId + " "+ transactionType+ " " + exchange.getIn().getHeader("kafka.KEY"));

                String response = responseBodyStart+ transactionAmont+ ", \"transactionCountry\":\""+ transactionCountry+ "\",\n" +
                        "\t\t\t\"merchantType\": "+ "\"MERCH336\"" + ",\n" +
                        "\t\t\t\"transactionType\":" + "\"Web\"" +" ,\"transactionId\":"+transactionId+",\n" +
                        "\"paymentMode\":\""+paymentMode+"\"},"+
                        "\t\t\"Customer\": {\n" +
                        "\t\t\t\"averageTransactionAmount\": 300,\n" +
                        "\t\t\t\"marriage\": false,\n" +
                        "\t\t\t\"jobChange\": false,\n" +
                        "\t\t\t\"cityChange\": false,\n" +
                        "\t\t\t\"customerId\": \""+exchange.getIn().getHeader("kafka.KEY")+"\"\n" +
                                              "\t\t\n" ;


                exchange.getIn().setHeader("responseStr",response);



        }
    }

    private final class KafkProcessor implements Processor {

        String endString=  "}}}";

        @Override
        public void process(Exchange exchange) throws Exception {
            System.out.println(exchange.getIn().getBody().toString());
            String responseStr = (String)exchange.getIn().getHeader("responseStr");
            com.fasterxml.jackson.databind.ObjectMapper objectMapper = new com.fasterxml.jackson.databind.ObjectMapper();

            java.util.HashMap valueMap = objectMapper.readValue(exchange.getIn().getBody().toString(), java.util.HashMap.class);
//
            java.util.Map dataMap = (java.util.HashMap)valueMap.get("data");
//
            java.util.List arr = (java.util.List) dataMap.get("ndarray");
//
            double prediction = (double)arr.get(0);


            String resp = responseStr+ ",\"riskIndex\":"+prediction+endString;
            exchange.getIn().setBody(resp);



        }
    }

    private final class GetPrediction implements Processor {

        String responseBodyStart = "{\n" +
                "\t\"data\": {\n" +
                "\t\t\"ndarray\": [\n" +
                "\t\t\t[1,0,";
        String responseBody = ",5000,0,0,0,0,";
        String responseEnd = ",0]\n" +
                "\t\t]\n" +
                "\t}\n" +
                "}";

        @Override
        public void process(Exchange exchange) throws Exception {

            Integer transactionAmount = (Integer)exchange.getIn().getHeader("transactionAmount");

            exchange.getIn().setBody(responseBodyStart+transactionAmount+responseBody+transactionAmount+responseEnd);


        }
    }
}
