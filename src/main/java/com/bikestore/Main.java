package com.bikestore;

import com.rabbitmq.client.*;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Main {
    private static final String EXCHANGE = "bikestore.orders.exchange";
    private static final String QUEUE_PAYMENT = "bikestore.payment.queue";
    private static final String QUEUE_EMAIL = "bikestore.email.queue";
    private static final String DLX = "bikestore.dlx";
    private static final String DLQ = "bikestore.payment.dlq";

    // Mapa en memoria para contar los reintentos (simulación local)
    private static final Map<String, Integer> retryMap = new ConcurrentHashMap<>();

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        // 1. Configurar Dead-Letter Exchange y Queue
        channel.exchangeDeclare(DLX, BuiltinExchangeType.DIRECT);
        channel.queueDeclare(DLQ, true, false, false, null);
        channel.queueBind(DLQ, DLX, "payment.failed");

        // 2. Configurar Exchange Principal y Colas de Trabajadores
        channel.exchangeDeclare(EXCHANGE, BuiltinExchangeType.FANOUT);

        Map<String, Object> paymentArgs = new HashMap<>();
        paymentArgs.put("x-dead-letter-exchange", DLX);
        paymentArgs.put("x-dead-letter-routing-key", "payment.failed");
        channel.queueDeclare(QUEUE_PAYMENT, true, false, false, paymentArgs);
        channel.queueBind(QUEUE_PAYMENT, EXCHANGE, "");

        channel.queueDeclare(QUEUE_EMAIL, true, false, false, null);
        channel.queueBind(QUEUE_EMAIL, EXCHANGE, "");

        log("System", "N/A", "Broker configurado. Iniciando Consumers...");
        
        startPaymentWorker(connection);
        startEmailWorker(connection);

        // Dar un segundo para que los workers se levanten
        Thread.sleep(1000);
        
        // 3. Simular peticiones desde el Front-end
        // Orden 1: Provocará reintentos y desvío a DLQ (50% de probabilidad por intento)
        startOrderProducer(connection, "ORD-8001", "PENDING");
        // Orden 2: Simula un cobro que ya viene autorizado, dispara el EmailWorker
        startOrderProducer(connection, "ORD-8002", "PAID");
    }

    private static void log(String step, String pedidoId, String msg) {
        String ts = new SimpleDateFormat("HH:mm:ss.SSS").format(new Date());
        String thread = Thread.currentThread().getName();
        System.out.printf("[%s] [Thread: %s] [Pedido: %s] [%s]: %s%n", ts, thread, pedidoId, step, msg);
    }

    private static void startOrderProducer(Connection conn, String id, String status) throws Exception {
        Channel channel = conn.createChannel();
        String json = String.format("{\"pedidoId\":\"%s\", \"paymentStatus\":\"%s\", \"amount\": 250.00}", id, status);
        log("OrderProducer", id, "Publicando orden al broker -> " + json);
        channel.basicPublish(EXCHANGE, "", null, json.getBytes(StandardCharsets.UTF_8));
    }

    private static void startPaymentWorker(Connection conn) throws Exception {
        Channel channel = conn.createChannel();
        channel.basicQos(1); // Procesar un mensaje a la vez

        DeliverCallback callback = (consumerTag, delivery) -> {
            String msg = new String(delivery.getBody(), StandardCharsets.UTF_8);
            String id = extractJsonField(msg, "pedidoId");
            
            int attempts = retryMap.getOrDefault(id, 0) + 1;
            retryMap.put(id, attempts);
            
            log("PaymentWorker", id, "Iniciando proceso de cobro (Intento " + attempts + ")");
            
            try {
                Thread.sleep(800); // Simulando latencia de pasarela
                if (Math.random() < 0.5) {
                    throw new RuntimeException("Rechazo bancario simulado");
                }
                log("PaymentWorker", id, "Cobro con tarjeta procesado exitosamente.");
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            } catch (Exception e) {
                if (attempts >= 3) {
                    log("PaymentWorker", id, "Fallo tras 3 intentos. Desviando mensaje a DLQ.");
                    // requeue = false envía el mensaje al Dead-Letter Exchange configurado
                    channel.basicReject(delivery.getEnvelope().getDeliveryTag(), false);
                } else {
                    log("PaymentWorker", id, "Error temporal. Reencolando mensaje para reintento.");
                    // requeue = true devuelve el mensaje a la cola original
                    channel.basicReject(delivery.getEnvelope().getDeliveryTag(), true);
                }
            }
        };
        channel.basicConsume(QUEUE_PAYMENT, false, callback, tag -> {});
    }

    private static void startEmailWorker(Connection conn) throws Exception {
        Channel channel = conn.createChannel();
        DeliverCallback callback = (consumerTag, delivery) -> {
            String msg = new String(delivery.getBody(), StandardCharsets.UTF_8);
            String id = extractJsonField(msg, "pedidoId");
            String status = extractJsonField(msg, "paymentStatus");
            
            if ("PAID".equals(status)) {
                log("EmailWorker", id, "Condición cumplida (paymentStatus=PAID). Enviando e-mail de confirmación.");
            } else {
                log("EmailWorker", id, "Ignorando evento (paymentStatus=" + status + "). No se envía e-mail.");
            }
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        };
        channel.basicConsume(QUEUE_EMAIL, false, callback, tag -> {});
    }
    
    // Utilidad rudimentaria para extraer campos JSON sin añadir dependencias externas como Jackson/Gson
    private static String extractJsonField(String json, String key) {
        try { 
            return json.split("\"" + key + "\":\"")[1].split("\"")[0]; 
        } catch (Exception e) { 
            return "UNKNOWN"; 
        }
    }
}