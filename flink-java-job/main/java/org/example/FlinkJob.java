package org.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.text.similarity.CosineSimilarity;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class FlinkJob {

    // --- Configuración ---
    private static final String KAFKA_BOOTSTRAP_SERVERS = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");
    private static final String DB_HOST = System.getenv().getOrDefault("DB_HOST", "db");
    private static final String DB_USER = System.getenv().getOrDefault("DB_USER", "postgres");
    private static final String DB_PASSWORD = System.getenv().getOrDefault("DB_PASSWORD", "postgres");
    private static final String DB_NAME = System.getenv().getOrDefault("DB_NAME", "db_consultas");
    private static final double SCORE_THRESHOLD = Double.parseDouble(System.getenv().getOrDefault("SCORE_THRESHOLD", "0.25"));
    private static final int MAX_QUALITY_RETRIES = Integer.parseInt(System.getenv().getOrDefault("MAX_QUALITY_RETRIES", "3"));

    private static final String TOPIC_RESPUESTAS_EXITOSAS = "respuestas_exitosas";
    private static final String TOPIC_PREGUNTAS_NUEVAS = "preguntas_nuevas";

    // Tags para separar el stream
    private static final OutputTag<JsonNode> DB_SINK_TAG = new OutputTag<JsonNode>("db-sink"){};
    private static final OutputTag<JsonNode> KAFKA_RETRY_SINK_TAG = new OutputTag<JsonNode>("kafka-retry-sink"){};

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1. Fuente de Datos (Kafka)
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
                .setTopics(TOPIC_RESPUESTAS_EXITOSAS)
                .setGroupId("flink-score-processor-group-java")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setProperty("client.dns.lookup", "use_all_dns_ips") // Solución para el problema de advertised listeners
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> kafkaStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // 2. Procesamiento y Bifurcación del Stream
        SingleOutputStreamOperator<JsonNode> processedStream = kafkaStream.process(new ScoreProcessor());

        // 3. Obtener los streams separados
        DataStream<JsonNode> dbStream = processedStream.getSideOutput(DB_SINK_TAG);
        DataStream<JsonNode> kafkaRetryStream = processedStream.getSideOutput(KAFKA_RETRY_SINK_TAG);

        // 4. Sinks (Destinos)

        // Sink para PostgreSQL
        dbStream.addSink(JdbcSink.sink(
                        "INSERT INTO preguntas (id, titulo, mejor_respuesta, respuesta_llm, score, numero_consultas) VALUES (?, ?, ?, ?, ?, 1) ON CONFLICT (id) DO NOTHING",
                        (statement, node) -> {
                            statement.setInt(1, node.get("indice_pregunta").asInt());
                            statement.setString(2, node.get("consulta").asText());
                            statement.setString(3, node.get("respuesta_popular").asText());
                            statement.setString(4, node.get("respuesta_llm").asText());
                            statement.setDouble(5, node.get("score").asDouble());
                        },
                        JdbcExecutionOptions.builder()
                                .withBatchIntervalMs(1000)
                                .withBatchSize(200)
                                .withMaxRetries(5)
                                .build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withDriverName("org.postgresql.Driver")
                                .withUrl(String.format("jdbc:postgresql://%s:5432/%s", DB_HOST, DB_NAME))
                                .withUsername(DB_USER)
                                .withPassword(DB_PASSWORD)
                                .build()
                )).name("PostgreSQL Sink");

        // Sink para reenviar a Kafka
        java.util.Properties producerProps = new java.util.Properties();
        producerProps.setProperty("client.dns.lookup", "use_all_dns_ips"); // Solución para el problema de advertised listeners

        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
                .setKafkaProducerConfig(producerProps)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(TOPIC_PREGUNTAS_NUEVAS)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                ).build();

        kafkaRetryStream.map((MapFunction<JsonNode, String>) JsonNode::toString) // Convertir JsonNode a String para Kafka
                        .sinkTo(kafkaSink)
                        .name("Kafka Retry Sink");

        // Ejecutar el job
        env.execute("Score Processing Job (Java)");
    }

    /**
     * Procesa cada mensaje, calcula el score y lo dirige al tag correspondiente.
     */
    public static class ScoreProcessor extends ProcessFunction<String, JsonNode> {
        private transient ObjectMapper jsonParser;

        @Override
        public void processElement(String value, Context ctx, Collector<JsonNode> out) throws Exception {
            if (jsonParser == null) {
                jsonParser = new ObjectMapper();
            }

            try {
                ObjectNode node = (ObjectNode) jsonParser.readTree(value);

                String llmAnswer = node.has("respuesta_llm") ? node.get("respuesta_llm").asText() : "";
                // El campo que contiene la respuesta de referencia es 'respuesta_popular'
                String bestAnswer = node.has("respuesta_popular") ? node.get("respuesta_popular").asText() : "";

                double score = tfidfCosineScore(llmAnswer, bestAnswer);
                node.put("score", score);

                if (score >= SCORE_THRESHOLD) {
                    // Score bueno -> a la DB
                    ctx.output(DB_SINK_TAG, node);
                } else {
                    // Score malo -> reintentar si es posible
                    int intentos = node.has("intentos_calidad") ? node.get("intentos_calidad").asInt() : 0;
                    if (intentos < MAX_QUALITY_RETRIES) {
                        node.put("intentos_calidad", intentos + 1);
                        ctx.output(KAFKA_RETRY_SINK_TAG, node);
                    } else {
                        // Máximo de reintentos alcanzado, se podría enviar a un tópico de fallidas.
                        // Por ahora, solo lo logueamos (Flink lo logueará si se configura).
                    }
                }
            } catch (Exception e) {
                // Loguear error, el mensaje se descarta.
                // logger.error("Error procesando mensaje: " + value, e);
            }
        }
    }

    /**
     * Calcula la similitud coseno usando una aproximación de TF-IDF.
     * Esta es una implementación simplificada.
     */
    private static double tfidfCosineScore(String text1, String text2) {
        if (text1 == null || text2 == null || text1.isEmpty() || text2.isEmpty()) {
            return 0.0;
        }

        Map<CharSequence, Integer> tf1 = getTermFrequency(text1);
        Map<CharSequence, Integer> tf2 = getTermFrequency(text2);

        CosineSimilarity cosineSimilarity = new CosineSimilarity();
        return cosineSimilarity.cosineSimilarity(tf1, tf2);
    }

    private static Map<CharSequence, Integer> getTermFrequency(String text) {
        Map<CharSequence, Integer> termFrequency = new HashMap<>();
        // Regex para separar por palabras, convirtiendo a minúsculas
        Pattern pattern = Pattern.compile("\\w+");
        java.util.regex.Matcher matcher = pattern.matcher(text.toLowerCase());
        while (matcher.find()) {
            String term = matcher.group();
            termFrequency.put(term, termFrequency.getOrDefault(term, 0) + 1);
        }
        return termFrequency;
    }

    /**
     * Convierte un String JSON a un objeto JsonNode.
     */
    public static class JsonStringToJsonNode implements MapFunction<String, JsonNode> {
        private transient ObjectMapper jsonParser;

        @Override
        public JsonNode map(String value) throws Exception {
            if (jsonParser == null) {
                jsonParser = new ObjectMapper();
            }
            return jsonParser.readTree(value);
        }
    }
}
