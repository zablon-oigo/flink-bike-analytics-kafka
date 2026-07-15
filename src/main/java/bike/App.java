package bike;

public class App {

    private static final String BOOTSTRAP = "localhost:29092";
    private static final String TOPIC = "bike.analytics";

    public static void main(String[] args) {

        Producer producer = new Producer(BOOTSTRAP, TOPIC);

        try {

            producer.publishBikeData();

            System.out.println("Finished publishing bike stations.");

        } catch (Exception e) {

            e.printStackTrace();

        } finally {

            producer.close();

        }
    }
}