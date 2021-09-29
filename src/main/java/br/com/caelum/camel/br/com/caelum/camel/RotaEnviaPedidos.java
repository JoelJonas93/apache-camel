package br.com.caelum.camel.br.com.caelum.camel;

import org.apache.activemq.camel.component.ActiveMQComponent;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Header;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.http4.HttpMethods;
import org.apache.camel.impl.DefaultCamelContext;
import org.xml.sax.SAXParseException;

public class RotaEnviaPedidos {

	public static void main(String[] args) throws Exception {

		CamelContext context = new DefaultCamelContext();
		context.addComponent("activemq", ActiveMQComponent.activeMQComponent("tcp://localhost:61616"));
		
		
		context.addRoutes(new RouteBuilder() {

			@Override
			public void configure() throws Exception {

				errorHandler(deadLetterChannel("activemq:queue:pedidos.DLQ").logExhaustedMessageHistory(true).maximumRedeliveries(3)
						.onRedelivery(new Processor() {

							@Override
							public void process(Exchange exchange) throws Exception {
								int counter = (int) exchange.getIn().getHeader(Exchange.REDELIVERY_COUNTER);
								int max = (int) exchange.getIn().getHeader(Exchange.REDELIVERY_MAX_COUNTER);
								// Redelivery 1 / 3
								System.out.println("Redelivery " + counter + " / " + max);
							}
						}));
				
				//deve ser configurado antes de qualquer rota
//				onException(SAXParseException.class).
//				    handled(true).
//				        maximumRedeliveries(3).
//				            redeliveryDelay(4000).
//				        onRedelivery(new Processor() {
//
//				            @Override
//				            public void process(Exchange exchange) throws Exception {
//				                    int counter = (int) exchange.getIn().getHeader(Exchange.REDELIVERY_COUNTER);
//				                    int max = (int) exchange.getIn().getHeader(Exchange.REDELIVERY_MAX_COUNTER);
//				                    System.out.println("Redelivery - " + counter + "/" + max );;
//				            }
//				    });
				
				from("activemq:queue:pedidos")
					.routeId("rota-pedidos")
					.to("validator:pedido.xsd").
//				from("file:pedidos?delay=5s&noop=true").routeId("rota-pedidos").to("validator:pedido.xsd");
				multicast().
					parallelProcessing().
						timeout(500).
							to("direct:soap").
							to("direct:http");

//				Staged event-driven architecture ou simplesmente SEDA,
//				com isso, o multicast se tornará desnecessário
//				routeId("rota-pedidos").
//			    to("seda:soap").
//			    to("seda:http");

				from("direct:http").routeId("rota-http").setProperty("pedidoId", xpath("/pedido/id/text()"))
						.setProperty("clienteId", xpath("/pedido/pagamento/email-titular/text()")).split()
						.xpath("/pedido/itens/item").setProperty("ebookId", xpath("/item/livro/codigo/text()")).filter()
						.xpath("/item/formato[text()='EBOOK']").marshal().xmljson().
				// log("${id} - ${body}").
				setHeader(Exchange.HTTP_METHOD, HttpMethods.GET).setHeader(Exchange.HTTP_QUERY, simple(
						"ebookId=${property.ebookId}&pedidoId=${property.pedidoId}&clienteId=${property.clienteId}"))
						.to("http4://localhost:8080/webservices/ebook/item");

				from("direct:soap").routeId("rota-soap").to("xslt:pedido-para-soap.xslt").log("${body}")
						.setHeader(Exchange.CONTENT_TYPE, constant("text/xml"))
						.to("http4://localhost:8080/webservices/financeiro");

			}
		});

		context.start();
		Thread.sleep(20000);
		context.stop();
	}

//	from("timer://negociacoes?fixedRate=true&delay=3s&period=360s").
//    to("http4://argentumws.caelum.com.br/negociacoes").
//      convertBodyTo(String.class).
//      unmarshal(new XStreamDataFormat(xStream)).
//      split(body()).
//      process(new Processor() {
//        @Override
//        public void process(Exchange exchange) throws Exception {
//            Negociacao negociacao = exchange.getIn().getBody(Negociacao.class);
//            exchange.setProperty("preco", negociacao.getPreco());
//            exchange.setProperty("quantidade", negociacao.getQuantidade());
//            String data = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss").format(negociacao.getData().getTime());
//            exchange.setProperty("data", data);
//        }
//      }).
//      setBody(simple("insert into negociacao(preco, quantidade, data) values (${property.preco}, ${property.quantidade}, '${property.data}')")).
//      log("${body}").
//      delay(1000).
//to("jdbc:mysql");
}
