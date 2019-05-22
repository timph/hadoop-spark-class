import java.util.stream.*;

public class Demo {
    var spark = SparkSession
    .builder()
    .appName('Some name')
    .config()
    ;

    public static int binary(int a, int b) { return a + b; }

    public static void associate() {
        int a = 1;
        int b = 2;
        int c = 3;

        // ASSOCIATIVE
        /*
        f(f(a, b), c) === f(a, f(b, c))
        */

        // Identity
        /*
        ID + a === a
        a + ID === a
        */
    }

    public static void leftFold() {
        long sum = 0;
        for (long i = 1; i <= 10000; i++)
            sum += i * i;
        System.out.println(sum);
    }

    public static void main(String[] args) {
        long streamer = LongStream
            .range(1, 10000)
            .parallel()
            .filter(i -> i % 2 == 1) // with SQL = select * from dataset where odd(data)
            .map(i -> i*i)
            .sum()
        ;
        System.out.println(streamer);
        leftFold();

    }
}