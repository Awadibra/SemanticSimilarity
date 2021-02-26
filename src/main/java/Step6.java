import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

public class Step6 {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        HashMap<String, String> pairsRelatedness;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            Region region = Region.US_EAST_1;
            S3Client s3 = S3Client.builder().region(region).build();
            s3.getObject(GetObjectRequest.builder().bucket("dsp-211-ass3").key("word-relatedness.txt").build(),
                    ResponseTransformer.toFile(Paths.get("pairsRelatedness.txt")));
            pairsRelatedness = new HashMap<>();
            File file = new File("pairsRelatedness.txt");
            try (BufferedReader br = new BufferedReader(new FileReader(file))) {
                String line;
                while ((line = br.readLine()) != null) {
                    Stemmer s1 = new Stemmer();
                    Stemmer s2 = new Stemmer();
                    String[] split = line.split("\\s+");
                    String first = split[0];
                    String second = split[1];
                    String relatedness = split[2];
                    s1.add(first.toCharArray(), first.length());
                    s1.stem();
                    s2.add(second.toCharArray(), second.length());
                    s2.stem();
                    String stemmedFirst = s1.toString();
                    String stemmedSecond = s2.toString();
                    pairsRelatedness.put(stemmedFirst + ":" + stemmedSecond, relatedness);
                }
            }
        }


        @Override
        public void map(LongWritable line, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("\t");
            String pair = split[0];
            String vector = split[1];
            String relatedness = pairsRelatedness.get(pair);
            vector += "," + relatedness;
//            context.write(new Text(vector), new Text());
            context.write(new Text(vector), new Text(pair));
            //vector,true/falue
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
        }

    }

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        int index;
        ArrayList<Integer> list;
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            list=new ArrayList<>(Arrays.asList(43,44,49));  //for analysis purpose
            index=0;
            context.write(new Text("@RELATION SemanticSimilarity"), new Text());
            for (int i = 0; i < 24; i++)
                context.write(new Text("@ATTRIBUTE value" + i + " REAL"), new Text());
            context.write(new Text("@ATTRIBUTE relatedness {True,False}"), new Text());
            context.write(new Text("@DATA"), new Text());

        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            if(list.contains(index)){
                for (Text t: values)
                System.out.println(key.toString() +" "+ t.toString()+" "+index);
            }
            context.write(key, new Text());
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "step6");
        job.setJarByClass(Step6.class);
        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
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

