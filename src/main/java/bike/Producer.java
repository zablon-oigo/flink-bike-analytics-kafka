package bike;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONArray;
import org.json.JSONObject;

import io.confluent.kafka.serializers.KafkaAvroSerializer;

public class Producer {

    private final KafkaProducer<String, BikeStation> producer;
    private final HttpClientService httpClient;
    private final String topic;

    public Producer(String bootstrapServers, String topic) {
        this.topic = topic;
        this.httpClient = new HttpClientService();

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put("schema.registry.url", "http://localhost:8081");

        this.producer = new KafkaProducer<>(props);
    }

    public void publishBikeData() throws Exception {

        String jsonResponse = httpClient.fetchBikeData();
        JSONArray stations = new JSONArray(jsonResponse);

        System.out.println("Stations found: " + stations.length());

        for (int i = 0; i < stations.length(); i++) {

            JSONObject station = stations.getJSONObject(i);

            String stationIdRaw = station.optString("id");
            String stationId = stationIdRaw.startsWith("BikePoints_")
                    ? stationIdRaw.substring("BikePoints_".length())
                    : stationIdRaw;

            String name = station.optString("commonName");

            double latitude = station.optDouble("lat", 0.0);
            double longitude = station.optDouble("lon", 0.0);

            int bikesAvailable = 0;
            int eBikes = 0;
            int emptyDocks = 0;
            int totalDocks = 0;

            JSONArray properties = station.optJSONArray("additionalProperties");

            if (properties != null) {
                for (int j = 0; j < properties.length(); j++) {

                    JSONObject prop = properties.getJSONObject(j);

                    String key = prop.optString("key");
                    String value = prop.optString("value", "0");

                    switch (key) {
                        case "NbBikes":
                            bikesAvailable = parseIntSafe(value);
                            break;

                        case "NbEBikes":
                            eBikes = parseIntSafe(value);
                            break;

                        case "NbEmptyDocks":
                            emptyDocks = parseIntSafe(value);
                            break;

                        case "NbDocks":
                            totalDocks = parseIntSafe(value);
                            break;
                    }
                }
            }

            BikeStation bikeStation = new BikeStation();

            bikeStation.setStationId(stationId);
            bikeStation.setStationName(name);
            bikeStation.setLatitude(latitude);
            bikeStation.setLongitude(longitude);
            bikeStation.setBikesAvailable(bikesAvailable);
            bikeStation.setEmptyDocks(emptyDocks);
            bikeStation.setTotalDocks(totalDocks);
            bikeStation.setEBikes(eBikes);
            bikeStation.setTimestamp(String.valueOf(System.currentTimeMillis()));

            ProducerRecord<String, BikeStation> producerRecord =
                    new ProducerRecord<>(topic, stationId, bikeStation);

            RecordMetadata metadata = producer.send(producerRecord).get();

            System.out.printf(
                    "Published %s -> topic=%s partition=%d offset=%d%n",
                    stationId,
                    metadata.topic(),
                    metadata.partition(),
                    metadata.offset());
        }

        producer.flush();
        System.out.println("Finished publishing bike stations.");
    }

    private int parseIntSafe(String value) {
        try {
            return Integer.parseInt(value);
        } catch (Exception e) {
            return 0;
        }
    }

    public void close() {
        producer.close();
    }
}