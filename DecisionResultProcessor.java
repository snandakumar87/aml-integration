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


public class DecisionResultProcessor extends RouteBuilder {
    private String consumerMaxPollRecords = "50000";
    private String consumerCount = "3";
    private String consumerSeekTo = "end";
    private String consumerGroup = "consumer-decision-processor";
    private  String kafkaBootstrap = "my-cluster-kafka-brokers:9092";

    @Override
    public void configure() throws Exception {

        GetCardTransactions getCardTransactions = new GetCardTransactions();
        from("kafka:" + "transaction-decision-response" + "?brokers=" + kafkaBootstrap + "&maxPollRecords="
                + consumerMaxPollRecords + "&seekTo=" + "end"
                + "&groupId=" + consumerGroup)
                .log("${body}")
                .process(getCardTransactions)
                .choice()
                .when(simple("${header.fraudAlert} != null"))
                .to("kafka:"+"fraud-topic"+ "?brokers=" + kafkaBootstrap)
                .to("kafka:"+"reporting-topic"+"?brokers=" + kafkaBootstrap)
                .otherwise()
                .to("kafka:"+"aml-topic"+ "?brokers=" + kafkaBootstrap)
                .to("kafka:"+"reporting-topic"+"?brokers=" + kafkaBootstrap);


    }


    private final class GetCardTransactions implements Processor {



        @Override
        public void process(Exchange exchange) throws Exception {

                java.util.Map valueMap = new com.fasterxml.jackson.databind.ObjectMapper().readValue(exchange.getIn().getBody().toString(), java.util.HashMap.class);

                java.util.Map creditInstanceMap = (java.util.HashMap) valueMap.get("data");
                System.out.println(creditInstanceMap.keySet());

            java.util.HashMap transactionMap = (java.util.HashMap) creditInstanceMap.get("Transaction");

            java.util.HashMap customerMap = (java.util.HashMap) creditInstanceMap.get("Customer");


                String fraudAlert= (String) creditInstanceMap.get("Fraud Alert");
                String amlAlert= String.valueOf(creditInstanceMap.get("Aml Alert"));

                exchange.getIn().setHeader("fraudAlert",fraudAlert);
                exchange.getIn().setHeader("amlAlert",amlAlert);

                System.out.println(transactionMap);
               System.out.println(customerMap);


            String responseString = "{ \"data\":{\"transactionId\":"+transactionMap.get("transactionId") + ", \"transactionAmount\":"+transactionMap.get("transactionAmount")+",\"paymentMode\":\""+transactionMap.get("paymentMode")+"\","
                        + "\"transactionCountry\":\""+transactionMap.get("transactionCountry")+"\",\"merchantType\":\""+ transactionMap.get("merchantType")+"\", \"customerRiskIndex\":\""+
                        customerMap.get("riskIndex")+"\",\"cityChange\":"+customerMap.get("cityChange") +",\"marriage\":"+customerMap.get("marriage")+",\"customerId\":\""+
                        customerMap.get("customerId")+"\",\"jobChange\":"+customerMap.get("jobChange")+",\"averageTransactionAmount\":"+customerMap.get("averageTransactionAmount")+",\"fraudAlert\":\""+
                        fraudAlert+"\",\"amlAlert\":\""+amlAlert+"\"}}";


                    System.out.println(responseString);
                    exchange.getIn().setBody(responseString);
                }


        }

    }

