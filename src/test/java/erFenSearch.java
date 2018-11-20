import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class erFenSearch {
    public static void main(String[] args) throws  IOException {
       HashMap<Long, String> map = new HashMap<Long, String>();
        ArrayList<Long> list = new ArrayList<>();
        String IP = "1.0.3.255";
        String[] split = IP.split("\\.");
        Long ip = ((Long.parseLong(split[0]) << 24) + (Long.parseLong(split[1]) << 16)
                + (Long.parseLong(split[2]) << 8) + (Long.parseLong(split[3])));
        System.out.println(ip);
        BufferedReader bufferedReader = new BufferedReader(new FileReader("F:\\ip_rules.txt"));
        String data = null;
        while ((data = bufferedReader.readLine()) != null) {
            String[] str = data.split(" ");
            String[] start_ip = str[0].split("\\.");
            String[] end_ip = str[1].split("\\.");
            long start = ((Long.parseLong(start_ip[0]) << 24) + (Long.parseLong(start_ip[1]) << 16)
                    + (Long.parseLong(start_ip[2]) << 8) + (Long.parseLong(start_ip[3])));
            long end = ((Long.parseLong(end_ip[0]) << 24) + (Long.parseLong(end_ip[1]) << 16)
                    + (Long.parseLong(end_ip[2]) << 8) + (Long.parseLong(end_ip[3])));

            String code = str[2];
            map.put(end, code);
            list.add(end);

        }
        System.out.println(list.toString());
        Long search = Search(list,ip);
        System.out.println(search);
        Set set = map.keySet();
        Iterator<Long> iterable =set.iterator();

        while (iterable.hasNext()){
            Long aLong = iterable.next();
            if(aLong>=search ){
                System.out.println(map.get(aLong));
            }
        }

    }
public static Long Search(ArrayList<Long> list ,Long ip){
        int low = 0;
        int high = list.size();
        int middle;
        while (low <= high) {
            middle = (low + high) / 2;
            if (list.get(middle) == ip) {
                return list.get(middle);
            }else if (ip < list.get(middle)) {
                high = middle - 1;
            } else  {
                low = middle + 1;
            }
        }
       return list.get(low-1);
    }
}
