To run solution, ensure maven is installed on your system, next:

1. Run `mvn install`. This will build the jar (java 8), copy it to the `data` folder, and run `docker-compose up`
2. Run `docker exec -it ws-data-spark_master_1 /bin/bash` to get into the container shell, and start utilizing Spark commands.
3. Run the job using `spark-submit --class Solution /tmp/data/ws-data-spark-1.0.jar`, this will output a solution csv for each problem to `/tmp/data` as well.

Tests are located in `src/test/java`.
