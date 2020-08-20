package cjohannsen.spark;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class Application {

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @Bean
    public CommandLineRunner commandLineRunner(ApplicationContext ctx) {
        return args -> {
            String logFile = "/home/cjohannsen/spark-3.0.0-bin-hadoop2.7/README.md"; // Should be some file on your system
            SparkSession spark = SparkSession.builder().appName("Simple Application").getOrCreate();
            Dataset<String> logData = spark.read().textFile(logFile).cache();

            long numAs = logData.filter((FilterFunction<String>) s -> s.contains("a")).count();
            long numBs = logData.filter((FilterFunction<String>) s -> s.contains("b")).count();

            System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);

            spark.stop();
        };
    }
}
