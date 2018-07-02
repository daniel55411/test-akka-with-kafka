package examples.kafka.example;

import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

public class BenchmarkRunner {
    public static void main(String[] args) throws RunnerException {
        Options options  =  new OptionsBuilder()
                .include(Benchmarks.class.getName())
                .mode(Mode.Throughput)
                .mode(Mode.SampleTime)
                .forks(2)
                .warmupIterations(5)
                .measurementIterations(10)
                .threads(12)
                .shouldDoGC(true)
                .build();

        new Runner(options).run();
    }
}
