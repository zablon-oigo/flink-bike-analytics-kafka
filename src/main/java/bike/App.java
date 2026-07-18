package bike;

public class App {

    private static final String BOOTSTRAP = "localhost:29092";
    private static final String TOPIC = "bike.analytics";

    public static void main(String[] args) {
        try {
            Producer producer = new Producer(BOOTSTRAP, TOPIC);

            try {
                producer.publishBikeData();
            } finally {
                producer.close();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
}
}