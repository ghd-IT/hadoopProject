public class demo {
    public static void main(String[] args) {
        int n = 2,m = 3,x,z = 1;
        x=n++ - --m + (z++ - n--);
        System.out.println(n+" "+m+" "+x+" "+z);
    }
}
