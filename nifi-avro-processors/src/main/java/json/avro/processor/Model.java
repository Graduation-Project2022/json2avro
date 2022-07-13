package json.avro.processor;

public class Model {

    private String numberA, numberB;
    private long timestamp;
    private int duration;

    public Model() {
    }

    public Model(String numberA, String numberB, long timestamp, int duration) {
        this.numberA = numberA;
        this.numberB = numberB;
        this.timestamp = timestamp;
        this.duration = duration;
    }

    public String getNumberA() {
        return numberA;
    }

    public void setNumberA(String numberA) {
        this.numberA = numberA;
    }

    public String getNumberB() {
        return numberB;
    }

    public void setNumberB(String numberB) {
        this.numberB = numberB;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public int getDuration() {
        return duration;
    }

    public void setDuration(int duration) {
        this.duration = duration;
    }
}

