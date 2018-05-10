public class POIMappingModel {
    private final double maxDensity;
    private final double maxRadius;
    private final double maxStdDeviation;
    private final double maxAvgDistance;
    private final int maxRequests;

    public POIMappingModel(int maxRequests, double maxAvgDistance, double maxStdDeviation, double maxRadius, double maxDensity) {
        this.maxRequests = maxRequests;
        this.maxAvgDistance = maxAvgDistance;
        this.maxStdDeviation = maxStdDeviation;
        this.maxRadius = maxRadius;
        this.maxDensity = maxDensity;
    }

    public int mapping(int requests, double avgDistance, double stdDeviation, double radius, double density) {
        return -10 + (int) Math.round((((requests/maxRequests)* 5)
                - ((stdDeviation/maxStdDeviation)* 10)) + ((density/maxDensity)* 5));
    }
}