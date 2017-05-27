import java.math.BigInteger;

public class Sample {

	public static void main(String[] args) {
		String a = "895686";
		String b = "989895";
		BigInteger aBig = new BigInteger(a);
		BigInteger bBig = new BigInteger(b);
		BigInteger sum = aBig.add(bBig);
		System.out.println("Big SUM: "+sum);
		String stringSum = sum.toString();
		System.out.println("String Sum: "+stringSum);

	}

}
