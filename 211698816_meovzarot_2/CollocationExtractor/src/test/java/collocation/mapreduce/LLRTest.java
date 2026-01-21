package collocation.mapreduce;

import org.junit.Test;
import static org.junit.Assert.*;

public class LLRTest {

    @Test
    public void testLLRCalculation() {
        // Example values.
        // C(w1) = 100
        // C(w2) = 200
        // C(w1, w2) = 50
        // N = 10000

        long c1 = 100;
        long c2 = 200;
        long c12 = 50;
        long N = 10000;

        // Expected LLR should be significant because 50 is much higher than expected
        // (100*200/10000 = 2).

        double llr = LLR.calculate(c1, c2, c12, N);

        System.out.println("LLR: " + llr);
        assertTrue(llr > 0);

        // Test Independent case (LLR should be close to 0?)
        // C(w1)=100, C(w2)=100, N=10000. Expected overlap = 1.
        // If we observe 1 overlap:
        long c12_indep = 1;
        double llr_indep = LLR.calculate(100, 100, c12_indep, 10000);
        System.out.println("LLR Indep: " + llr_indep);

        // Test Zero overlap (should be valid and maybe negative association but LLR is
        // G-test, usually positive for any deviation?)
        // Actually LLR is always positive.
        // It measures deviation from independence.

        double llr_zero = LLR.calculate(100, 100, 0, 10000); // 0 overlap vs expected 1
        System.out.println("LLR Zero: " + llr_zero);
        assertTrue(llr_zero >= 0);
    }
}
