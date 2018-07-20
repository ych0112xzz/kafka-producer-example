package producer_file;

import java.util.Random;

public class TestMain {
    public static void main(String[] args) {
        Random random = new Random(47);
        for (int i = 0; i < 10; i++) {
            int p = random.nextInt(5);
            System.out.println(p);
        }
    }
}
