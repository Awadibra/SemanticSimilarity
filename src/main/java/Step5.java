import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;

public class Step5 {

    public static class MapperClass extends Mapper<Text, MapWritable, Text, MapWritable> {
        HashMap<String, ArrayList<String>> pairs;
        HashMap<String, ArrayList<String>> invertPairs;


        //v1
        @Override
        public void setup(Context context) throws IOException {
            Region region = Region.US_EAST_1;
            S3Client s3 = S3Client.builder().region(region).build();
            s3.getObject(GetObjectRequest.builder().bucket("dsp-211-ass3").key("word-relatedness.txt").build(),
                    ResponseTransformer.toFile(Paths.get("pairs.txt")));
            pairs = new HashMap<>();
            invertPairs = new HashMap<>();

            File file = new File("pairs.txt");
            try (BufferedReader br = new BufferedReader(new FileReader(file))) {
                String line;
                while ((line = br.readLine()) != null) {
                    Stemmer s1 = new Stemmer();
                    Stemmer s2 = new Stemmer();
                    String[] split = line.split("\\s+");
                    String first = split[0];
                    String second = split[1];
                    s1.add(first.toCharArray(), first.length());
                    s1.stem();
                    s2.add(second.toCharArray(), second.length());
                    s2.stem();
                    String stemmedFirst = s1.toString();
                    String stemmedSecond = s2.toString();
                    if (!pairs.containsKey(stemmedFirst)) {
                        pairs.put(stemmedFirst, new ArrayList<String>());
                    }
                    pairs.get(stemmedFirst).add(stemmedSecond);
                }
            }
            for (String word : pairs.keySet()) {
                for (String second : pairs.get(word)) {
                    if (!invertPairs.containsKey(second)) {
                        invertPairs.put(second, new ArrayList<>());
                    }
                    invertPairs.get(second).add(word);
                }
            }
//            System.out.println("PAIRS:");
//            for (String word : pairs.keySet()) {
//                for (String second : pairs.get(word)) {
//                    System.out.println(word + "\t" + second);
//                }
//            }
//            System.out.println("INVERTED PAIRS:");
//            for (String word : invertPairs.keySet()) {
//                for (String second : invertPairs.get(word)) {
//                    System.out.println(word + "\t" + second);
//                }
//            }
        }

        @Override
        public void map(Text key, MapWritable value, Context context) throws IOException, InterruptedException {
            String word = key.toString();
//            System.out.println("word is: " + word);
            ArrayList<String> list1 = pairs.get(word);
            ArrayList<String> list2 = invertPairs.get(word);
            MapWritable map = new MapWritable(value);
            if (list1 != null) {
                for (String sec : list1) {
                    context.write(new Text(word + ":" + sec), map);
                }
            }
            if (list2 != null) {
                for (String sec : list2) {
                    context.write(new Text(sec + ":" + word), map);
                }
            }

            //alligator:dog alligatorVector
            //alligator:dog dogVector
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
        }

    }

    public static class ReducerClass extends Reducer<Text, MapWritable, Text, Text> {
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
        }

        @Override
        public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
            Iterator<MapWritable> it = values.iterator();
//            System.out.println("key: " + key.toString());
            List<MapWritable> cache = new ArrayList<>();
            while (it.hasNext()) {
//                System.out.println("key: "+key);
                MapWritable next = new MapWritable(it.next());
//                System.out.println("map added to cache:");
//                for (Map.Entry<Writable, Writable> in : next.entrySet()) {
//                    System.out.println(in.getKey()+" "+in.getValue());
//                }
                cache.add(next);
            }
            if (cache.size() == 2) {
//                System.out.println("before sync");
                MapWritable l1 = new MapWritable(cache.get(0));
                MapWritable l2 = new MapWritable(cache.get(1));
//                for (Map.Entry entry : l1.entrySet()) {
//                    System.out.println(entry.getKey());
//                    System.out.println(entry.getValue());
//                }
                if (key.toString().equals("bomber:bomb")) {
                    for (Writable in : l1.keySet()) {
                        System.out.println(l1.get(in).toString() + " " + l2.get(in).toString());
                    }
                }


//                Set<Writable> sourceKeysList = new HashSet<>(l1.keySet());
//                Set<Writable> targetKeysList = new HashSet<>(l2.keySet());
//                Set<Writable> sourceValuesList = new HashSet<>(l1.values());
//                Set<Writable> targetValuesList = new HashSet<>(l2.values());
//                System.out.println("l1 and l2 not common keys");
//                System.out.println(sourceKeysList.removeAll(targetKeysList));
//                System.out.println("l1 and l2 common values");
//                System.out.println(sourceValuesList.retainAll(targetValuesList));


                //syncronize maps
//                System.out.println("++++++++ "+ key.toString() +" ++++++++");
//                System.out.println("before sync: l1: "+l1.size()+" l2: "+l2.size());
//                System.out.println("does l1 key set equals to l2? "+ l1.keySet().equals( l2.keySet() ));
//                System.out.println("does l1 values equals to l2? "+ l1.values().equals( l2.values() ));
//                System.out.println("does l1 equals to l2? "+ l1.equals(l2));

                syncMaps(l1, l2);

//                System.out.println("after sync: l1: "+l1.size()+" l2: "+l2.size());
//                System.out.println("does l1 key set equals to l2? "+ l1.keySet().equals( l2.keySet() ));
//                System.out.println("does l1 values equals to l2? "+ l1.values().equals( l2.values() ));
//                System.out.println("does l1 equals to l2? "+ l1.equals(l2));

//                System.out.println("after sync");
//                for (Map.Entry entry : l1.entrySet()) {
//                    System.out.println(entry.getKey());
//                    System.out.println(entry.getValue());
//                }
                double[] vector = calcVector(l1, l2);
//                System.out.println("VECTOR of: " + key.toString());
//                System.out.println(Arrays.toString(vector));
                String[] vec = new String[24];
                for (int i = 0; i < 24; i++) {
//                    vec[i] = String.format("%.20f", vector[i]);
                    vec[i] = String.valueOf(vector[i]);
                }
//                System.out.println(Arrays.toString(vec));
                String v = String.join(",", vec);
                context.write(key, new Text(v));

            }
            //alligator:frog    24vector
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "step5");
        job.setJarByClass(Step5.class);
        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static void syncMaps(MapWritable l1, MapWritable l2) {
//        System.out.println("in sync: l1: "+l1.size()+" l2: "+l2.size());
//        System.out.println("does l1 equals to l2? "+ l1.keySet().equals( l2.keySet() ));  //true

        for (Writable in : l1.keySet()) {
//            System.out.println("does l2 contains "+in +"? "+l2.containsKey(in)+ "and "+in.toString()+"? " +l2.containsKey(in.toString()));
            if (!l2.containsKey(in)) {
                l2.put(in, new Text("0:0:0:0"));
//                System.out.println(in + " added to l2 with 0:0:0:0");
            }
        }
        for (Writable in : l2.keySet()) {
            if (!l1.containsKey(in)) {
                l1.put(in, new Text("0:0:0:0"));
//                System.out.println(in + " added to l1 with 0:0:0:0");
            }
        }

    }

    private static double eq9X(MapWritable l1, MapWritable l2, int index) {
//        System.out.println("eq9:");
        Set<Writable> keys = l1.keySet();
//        System.out.println(Arrays.toString(keys.toArray()));
        double sum = 0.0;
        for (Writable k : keys) {
            String t1 = l1.get(k).toString();
            String t2 = l2.get(k).toString();
            String[] split1 = t1.split(":");
            String[] split2 = t2.split(":");
            double v1 = Double.parseDouble(split1[index]);
            double v2 = Double.parseDouble(split2[index]);
            sum += Math.abs(v1 - v2);
        }
//        System.out.println("sum: " + sum);
        return sum;
    }

    private static double eq10X(MapWritable l1, MapWritable l2, int index) {
//        System.out.println("eq10:");
        Set<Writable> keys = l1.keySet();
        double sum = 0.0;
        for (Writable k : keys) {
            String t1 = l1.get(k).toString();
            String t2 = l2.get(k).toString();
            String[] split1 = t1.split(":");
            String[] split2 = t2.split(":");
            double v1 = Double.parseDouble(split1[index]);
            double v2 = Double.parseDouble(split2[index]);
            sum += Math.pow(v1 - v2, 2);
        }
//        System.out.println("sum: " + Math.sqrt(sum));

        return Math.sqrt(sum);
    }

    private static double eq11X(MapWritable l1, MapWritable l2, int index) {
//        System.out.println("eq11:");
        Set<Writable> keys = l1.keySet();
        double sum = 0.0;
        double e1 = 0.0;
        double e2 = 0.0;
        for (Writable k : keys) {
            String t1 = l1.get(k).toString();
            String t2 = l2.get(k).toString();
            String[] split1 = t1.split(":");
            String[] split2 = t2.split(":");
            double v1 = Double.parseDouble(split1[index]);
            e1 += (v1 * v1);
            double v2 = Double.parseDouble(split2[index]);
            e2 += (v2 * v2);
            sum += (v1 * v2);
        }
        e1 = Math.sqrt(e1);
        e2 = Math.sqrt(e2);

//        System.out.println("sum: " + sum / (e1 * e2));

        return sum / (e1 * e2);
    }

    private static double eq13X(MapWritable l1, MapWritable l2, int index) {
//        System.out.println("eq13:");

        Set<Writable> keys = l1.keySet();
        double sum1 = 0.0;
        double sum2 = 0.0;
        for (Writable k : keys) {
            String t1 = l1.get(k).toString();
            String t2 = l2.get(k).toString();
            String[] split1 = t1.split(":");
            String[] split2 = t2.split(":");
            double v1 = Double.parseDouble(split1[index]);
            double v2 = Double.parseDouble(split2[index]);
            sum1 += Math.min(v1, v2);
            sum2 += Math.max(v1, v2);
        }
//        System.out.println("sum: " + sum1 / sum2);

        return sum1 / sum2;
    }

    private static double eq15X(MapWritable l1, MapWritable l2, int index) {
//        System.out.println("eq15:");

        Set<Writable> keys = l1.keySet();
        double sum1 = 0.0;
        double sum2 = 0.0;
        for (Writable k : keys) {
            String t1 = l1.get(k).toString();
            String t2 = l2.get(k).toString();
            String[] split1 = t1.split(":");
            String[] split2 = t2.split(":");
            double v1 = Double.parseDouble(split1[index]);
            double v2 = Double.parseDouble(split2[index]);
            sum1 += Math.min(v1, v2);
            sum2 += (v1 + v2);
        }
//        System.out.println("sum: " + (2 * sum1) / sum2);

        return (2 * sum1) / sum2;
    }

    private static double eq17X(MapWritable l1, MapWritable l2, int index) {
        System.out.println("eq17:");

        Set<Writable> keys = l1.keySet();
//        System.out.println("l1 size: "+keys.size());

        double[] average = new double[keys.size()];
        int i = 0;
        double[] l1arr = new double[keys.size()];
        double[] l2arr = new double[keys.size()];
        for (Writable k : keys) {
            String t1 = l1.get(k).toString();
            String t2 = l2.get(k).toString();
//                System.out.println("t1 " + t1 + " t2 " + t2);

            String[] split1 = t1.split(":");
            String[] split2 = t2.split(":");
            double v1 = Double.parseDouble(split1[index]);
            double v2 = Double.parseDouble(split2[index]);

//                System.out.println("v1 " + v1 + " v2 " + v2);
            l1arr[i] = v1;
            l2arr[i] = v2;
            average[i] = (v1 + v2) / 2;
            i++;
        }
        double ret = (eq16(l1arr, average) + eq16(l2arr, average));
        System.out.println("sum: " + ret);

        return ret;
    }

    private static double eq16(double[] arr, double[] average) {
        System.out.println("eq16:");
        System.out.println(Arrays.toString(arr));
        System.out.println(Arrays.toString(average));
        double res = 0.0;
        for (int i = 0; i < arr.length; ++i) {
            if (arr[i] == 0 || average[i] == 0.0) {
                continue;
            }
            System.out.println("arr[i] " + arr[i] + " Math.log " + Math.log(arr[i] / average[i]));
            double eq = (arr[i] * Math.log(arr[i] / average[i]));
            res += eq;
        }

        System.out.println("sum: " + res / Math.log(2));

        return res / Math.log(2);

    }

    private static double[] calcVector(MapWritable l1, MapWritable l2) {
        double[] vector = new double[24];
        int index = 0;
        //eq9
        for (int i = 0; i < 4; i++) {
            vector[index] = eq9X(l1, l2, i);
            index++;
        }
//        index++;
        //eq10
        for (int i = 0; i < 4; i++) {
            vector[index] = eq10X(l1, l2, i);
            index++;
        }
        //e11
//        index++;
        for (int i = 0; i < 4; i++) {
            vector[index] = eq11X(l1, l2, i);
            index++;
        }
        //eq13
//        index++;
        for (int i = 0; i < 4; i++) {
            vector[index] = eq13X(l1, l2, i);
            index++;
        }
        //eq15
//        index++;
        for (int i = 0; i < 4; i++) {
            vector[index] = eq15X(l1, l2, i);
            index++;
        }
        //eq17
//        index++;
        for (int i = 0; i < 4; i++) {
            vector[index] = eq17X(l1, l2, i);
            index++;
        }
        return vector;
    }


    private static class Stemmer {
        private char[] b;
        private int i,     /* offset into b */
                i_end, /* offset to end of stemmed word */
                j, k;
        private static final int INC = 50;

        /* unit of size whereby b is increased */
        public Stemmer() {
            b = new char[INC];
            i = 0;
            i_end = 0;
        }

        /**
         * Add a character to the word being stemmed.  When you are finished
         * adding characters, you can call stem(void) to stem the word.
         */

        public void add(char ch) {
            if (i == b.length) {
                char[] new_b = new char[i + INC];
                for (int c = 0; c < i; c++) new_b[c] = b[c];
                b = new_b;
            }
            b[i++] = ch;
        }


        /**
         * Adds wLen characters to the word being stemmed contained in a portion
         * of a char[] array. This is like repeated calls of add(char ch), but
         * faster.
         */

        public void add(char[] w, int wLen) {
            if (i + wLen >= b.length) {
                char[] new_b = new char[i + wLen + INC];
                for (int c = 0; c < i; c++) new_b[c] = b[c];
                b = new_b;
            }
            for (int c = 0; c < wLen; c++) b[i++] = w[c];
        }

        /**
         * After a word has been stemmed, it can be retrieved by toString(),
         * or a reference to the internal buffer can be retrieved by getResultBuffer
         * and getResultLength (which is generally more efficient.)
         */
        public String toString() {
            return new String(b, 0, i_end);
        }

        /**
         * Returns the length of the word resulting from the stemming process.
         */
        public int getResultLength() {
            return i_end;
        }

        /**
         * Returns a reference to a character buffer containing the results of
         * the stemming process.  You also need to consult getResultLength()
         * to determine the length of the result.
         */
        public char[] getResultBuffer() {
            return b;
        }

        /* cons(i) is true <=> b[i] is a consonant. */

        private final boolean cons(int i) {
            switch (b[i]) {
                case 'a':
                case 'e':
                case 'i':
                case 'o':
                case 'u':
                    return false;
                case 'y':
                    return (i == 0) ? true : !cons(i - 1);
                default:
                    return true;
            }
        }

   /* m() measures the number of consonant sequences between 0 and j. if c is
      a consonant sequence and v a vowel sequence, and <..> indicates arbitrary
      presence,

         <c><v>       gives 0
         <c>vc<v>     gives 1
         <c>vcvc<v>   gives 2
         <c>vcvcvc<v> gives 3
         ....
   */

        private final int m() {
            int n = 0;
            int i = 0;
            while (true) {
                if (i > j) return n;
                if (!cons(i)) break;
                i++;
            }
            i++;
            while (true) {
                while (true) {
                    if (i > j) return n;
                    if (cons(i)) break;
                    i++;
                }
                i++;
                n++;
                while (true) {
                    if (i > j) return n;
                    if (!cons(i)) break;
                    i++;
                }
                i++;
            }
        }

        /* vowelinstem() is true <=> 0,...j contains a vowel */

        private final boolean vowelinstem() {
            int i;
            for (i = 0; i <= j; i++) if (!cons(i)) return true;
            return false;
        }

        /* doublec(j) is true <=> j,(j-1) contain a double consonant. */

        private final boolean doublec(int j) {
            if (j < 1) return false;
            if (b[j] != b[j - 1]) return false;
            return cons(j);
        }

   /* cvc(i) is true <=> i-2,i-1,i has the form consonant - vowel - consonant
      and also if the second c is not w,x or y. this is used when trying to
      restore an e at the end of a short word. e.g.

         cav(e), lov(e), hop(e), crim(e), but
         snow, box, tray.

   */

        private final boolean cvc(int i) {
            if (i < 2 || !cons(i) || cons(i - 1) || !cons(i - 2)) return false;
            {
                int ch = b[i];
                if (ch == 'w' || ch == 'x' || ch == 'y') return false;
            }
            return true;
        }

        private final boolean ends(String s) {
            int l = s.length();
            int o = k - l + 1;
            if (o < 0) return false;
            for (int i = 0; i < l; i++) if (b[o + i] != s.charAt(i)) return false;
            j = k - l;
            return true;
        }

   /* setto(s) sets (j+1),...k to the characters in the string s, readjusting
      k. */

        private final void setto(String s) {
            int l = s.length();
            int o = j + 1;
            for (int i = 0; i < l; i++) b[o + i] = s.charAt(i);
            k = j + l;
        }

        /* r(s) is used further down. */

        private final void r(String s) {
            if (m() > 0) setto(s);
        }

   /* step1() gets rid of plurals and -ed or -ing. e.g.

          caresses  ->  caress
          ponies    ->  poni
          ties      ->  ti
          caress    ->  caress
          cats      ->  cat

          feed      ->  feed
          agreed    ->  agree
          disabled  ->  disable

          matting   ->  mat
          mating    ->  mate
          meeting   ->  meet
          milling   ->  mill
          messing   ->  mess

          meetings  ->  meet

   */

        private final void step1() {
            if (b[k] == 's') {
                if (ends("sses")) k -= 2;
                else if (ends("ies")) setto("i");
                else if (b[k - 1] != 's') k--;
            }
            if (ends("eed")) {
                if (m() > 0) k--;
            } else if ((ends("ed") || ends("ing")) && vowelinstem()) {
                k = j;
                if (ends("at")) setto("ate");
                else if (ends("bl")) setto("ble");
                else if (ends("iz")) setto("ize");
                else if (doublec(k)) {
                    k--;
                    {
                        int ch = b[k];
                        if (ch == 'l' || ch == 's' || ch == 'z') k++;
                    }
                } else if (m() == 1 && cvc(k)) setto("e");
            }
        }

        /* step2() turns terminal y to i when there is another vowel in the stem. */

        private final void step2() {
            if (ends("y") && vowelinstem()) b[k] = 'i';
        }

   /* step3() maps double suffices to single ones. so -ization ( = -ize plus
      -ation) maps to -ize etc. note that the string before the suffix must give
      m() > 0. */

        private final void step3() {
            if (k == 0) return; /* For Bug 1 */
            switch (b[k - 1]) {
                case 'a':
                    if (ends("ational")) {
                        r("ate");
                        break;
                    }
                    if (ends("tional")) {
                        r("tion");
                        break;
                    }
                    break;
                case 'c':
                    if (ends("enci")) {
                        r("ence");
                        break;
                    }
                    if (ends("anci")) {
                        r("ance");
                        break;
                    }
                    break;
                case 'e':
                    if (ends("izer")) {
                        r("ize");
                        break;
                    }
                    break;
                case 'l':
                    if (ends("bli")) {
                        r("ble");
                        break;
                    }
                    if (ends("alli")) {
                        r("al");
                        break;
                    }
                    if (ends("entli")) {
                        r("ent");
                        break;
                    }
                    if (ends("eli")) {
                        r("e");
                        break;
                    }
                    if (ends("ousli")) {
                        r("ous");
                        break;
                    }
                    break;
                case 'o':
                    if (ends("ization")) {
                        r("ize");
                        break;
                    }
                    if (ends("ation")) {
                        r("ate");
                        break;
                    }
                    if (ends("ator")) {
                        r("ate");
                        break;
                    }
                    break;
                case 's':
                    if (ends("alism")) {
                        r("al");
                        break;
                    }
                    if (ends("iveness")) {
                        r("ive");
                        break;
                    }
                    if (ends("fulness")) {
                        r("ful");
                        break;
                    }
                    if (ends("ousness")) {
                        r("ous");
                        break;
                    }
                    break;
                case 't':
                    if (ends("aliti")) {
                        r("al");
                        break;
                    }
                    if (ends("iviti")) {
                        r("ive");
                        break;
                    }
                    if (ends("biliti")) {
                        r("ble");
                        break;
                    }
                    break;
                case 'g':
                    if (ends("logi")) {
                        r("log");
                        break;
                    }
            }
        }

        /* step4() deals with -ic-, -full, -ness etc. similar strategy to step3. */

        private final void step4() {
            switch (b[k]) {
                case 'e':
                    if (ends("icate")) {
                        r("ic");
                        break;
                    }
                    if (ends("ative")) {
                        r("");
                        break;
                    }
                    if (ends("alize")) {
                        r("al");
                        break;
                    }
                    break;
                case 'i':
                    if (ends("iciti")) {
                        r("ic");
                        break;
                    }
                    break;
                case 'l':
                    if (ends("ical")) {
                        r("ic");
                        break;
                    }
                    if (ends("ful")) {
                        r("");
                        break;
                    }
                    break;
                case 's':
                    if (ends("ness")) {
                        r("");
                        break;
                    }
                    break;
            }
        }

        /* step5() takes off -ant, -ence etc., in context <c>vcvc<v>. */

        private final void step5() {
            if (k == 0) return; /* for Bug 1 */
            switch (b[k - 1]) {
                case 'a':
                    if (ends("al")) break;
                    return;
                case 'c':
                    if (ends("ance")) break;
                    if (ends("ence")) break;
                    return;
                case 'e':
                    if (ends("er")) break;
                    return;
                case 'i':
                    if (ends("ic")) break;
                    return;
                case 'l':
                    if (ends("able")) break;
                    if (ends("ible")) break;
                    return;
                case 'n':
                    if (ends("ant")) break;
                    if (ends("ement")) break;
                    if (ends("ment")) break;
                    /* element etc. not stripped before the m */
                    if (ends("ent")) break;
                    return;
                case 'o':
                    if (ends("ion") && j >= 0 && (b[j] == 's' || b[j] == 't')) break;
                    /* j >= 0 fixes Bug 2 */
                    if (ends("ou")) break;
                    return;
                /* takes care of -ous */
                case 's':
                    if (ends("ism")) break;
                    return;
                case 't':
                    if (ends("ate")) break;
                    if (ends("iti")) break;
                    return;
                case 'u':
                    if (ends("ous")) break;
                    return;
                case 'v':
                    if (ends("ive")) break;
                    return;
                case 'z':
                    if (ends("ize")) break;
                    return;
                default:
                    return;
            }
            if (m() > 1) k = j;
        }

        /* step6() removes a final -e if m() > 1. */

        private final void step6() {
            j = k;
            if (b[k] == 'e') {
                int a = m();
                if (a > 1 || a == 1 && !cvc(k - 1)) k--;
            }
            if (b[k] == 'l' && doublec(k) && m() > 1) k--;
        }

        /**
         * Stem the word placed into the Stemmer buffer through calls to add().
         * Returns true if the stemming process resulted in a word different
         * from the input.  You can retrieve the result with
         * getResultLength()/getResultBuffer() or toString().
         */
        public void stem() {
            k = i - 1;
            if (k > 1) {
                step1();
                step2();
                step3();
                step4();
                step5();
                step6();
            }
            i_end = k + 1;
            i = 0;
        }

        public void clear() {

        }
    }
}