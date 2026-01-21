package collocation.mapreduce;

public class LLR {

    public static double calculate(long c1, long c2, long c12, long N) {

        // Check for validity
        if (c12 <= 0 || c1 <= 0 || c2 <= 0 || N <= 0)
            return 0.0;

        double k11 = c12;
        double k12 = c1 - c12;
        double k21 = c2 - c12;
        double k22 = N - c1 - c2 + c12;

        if (k12 < 0 || k21 < 0 || k22 < 0) {
            // Should not happen if data is consistent
            return 0.0;
        }

        return 2 * (entropy(k11, k12, k21, k22)
                - entropy(k11 + k12, k21 + k22)
                - entropy(k11 + k21, k12 + k22));
    }

    private static double entropy(double... elements) {
        double sum = 0;
        for (double element : elements) {
            sum += element;
        }
        double result = 0;
        for (double x : elements) {
            if (x > 0) { // log(0) is undefined, lim x->0 xlogx = 0
                result += x * Math.log(x / sum);
            }
        }
        return result; // H returns negative sum p log p.
        // LLR formula usually: 2 * (L(Model1) - L(Model2)).
        // L = sum k_i * log(p_i)
        // With H: sum k_i log(k_i/N) = sum k_i log k_i - N log N.

        /*
         * Alternative Standard Form:
         * 2 * (matrix_sum * H(matrix) - row_sum*H(rows) - col_sum*H(cols))
         * where H(X) = - sum p log p
         * 
         * Let's stick to the log likelihood addition form:
         * LL = sum(k * log(k/n))
         * LLR = 2 * (LL(p1, k1, n1) + ... - ...)
         */

        // This entropy helper returns sum x log (x/sum).
        // Which is - N * H(p).
        // Correct for the formula 2 * (LogL_Alternative - LogL_Null).
    }
}
