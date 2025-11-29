package experimentEngine;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.time.Instant;
import java.time.Duration;
import java.util.stream.Collectors;

import experimentEngine.DataFrame7.Order;

/**
 * DataFrame13.java (Includes Predicate Engine, Joins, saveCsv, SELECT/Projection, and Dynamic FROMCSV)
 */
public class DataFrame13 {

    // --- EXECUTION SETTINGS ---
    private static final int PARALLELISM_LEVEL = Runtime.getRuntime().availableProcessors();
    private transient final ExecutorService queryThreadPool = Executors.newFixedThreadPool(PARALLELISM_LEVEL);
    private static final int VECTOR_CHUNK_SIZE = 8;
    
    // Define the name of the metadata file
    private static final String METADATA_FILE = "metadata.dat";

    // --- HELPER CLASSES (ALL must be static and Serializable where needed) ---
    private enum Encoding { CONSTANT, RLE, BITPACK }

    static class Pair<L, R> implements Serializable {
        private static final long serialVersionUID = 1L;
        final L left; final R right;
        Pair(L left, R right) { this.left = left; this.right = right; }
        @Override public int hashCode() { return Objects.hash(left, right); }
        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Pair<?, ?> pair = (Pair<?, ?>) o;
            return Objects.equals(left, pair.left) && Objects.equals(right, pair.right);
        }
    }
    public static class MATCHED implements Serializable {
        private static final long serialVersionUID = 1L;
        public List<Pair<Integer, Integer>> matches = new ArrayList<>();
    }

    static class BitmapIndex implements Serializable {
        private static final long serialVersionUID = 1L;
        private final Map<Integer, BitSet> index = new HashMap<>();
        public void set(int dictId, int rowLocalIndex) { index.computeIfAbsent(dictId, k -> new BitSet()).set(rowLocalIndex); }
        public BitSet get(int dictId) { return index.getOrDefault(dictId, new BitSet()); }
        public Set<Integer> getAllIds() { return index.keySet(); }
    }
    
    static class SegmentHeader implements Serializable {
        private static final long serialVersionUID = 1L;
        final long sum; final int min, max;
        final long fileOffset; final int fileLength;
        final int count; final Encoding enc; final int bits;
        final BitmapIndex bitmapIndex;
        final Set<Integer> presentIds; 

        SegmentHeader(Segment seg) {
            this.sum = seg.sum; this.min = seg.min; this.max = seg.max;
            this.fileOffset = seg.fileOffset; this.fileLength = seg.fileLength;
            this.count = seg.count; this.enc = seg.enc; this.bits = seg.bits;
            this.bitmapIndex = seg.bitmapIndex; 
            this.presentIds = seg.presentIds;
        }
    }
    
    static class Segment {
        Encoding enc; int bits; int count; long sum; int min, max;
        Set<Integer> presentIds = new HashSet<>();
        long fileOffset; int fileLength; BitmapIndex bitmapIndex = null;
    }
    
    // --- PREDICATE CLASSES FOR BOOLEAN LOGIC ---
    public abstract static class LOGIC implements Serializable {
        private static final long serialVersionUID = 1L;
        public abstract BitSet evaluate(DataFrame13 df, String primaryColumn, int segmentIndex);
    }
    
    public enum OPERATOR { EQ, LT, LE, GT, GE, NE }
    
    public static class COMPARE extends LOGIC {
        private static final long serialVersionUID = 1L;
        final String column; final Object value; final OPERATOR op;
        public COMPARE(String column, Object value, OPERATOR op) { 
            this.column = column; this.value = value; this.op = op; 
        }
        
        @Override 
        public BitSet evaluate(DataFrame13 df, String primaryColumn, int segmentIndex) {
            return df.evaluateSegment(this, segmentIndex);
        }
    }

    public static class AND extends LOGIC {
        private static final long serialVersionUID = 1L;
        final LOGIC left; final LOGIC right;
        public AND(LOGIC left, LOGIC right) { 
            this.left = left; this.right = right; 
        }
        
        @Override 
        public BitSet evaluate(DataFrame13 df, String primaryColumn, int segmentIndex) {
            BitSet leftResult = left.evaluate(df, primaryColumn, segmentIndex);
            BitSet rightResult = right.evaluate(df, primaryColumn, segmentIndex);
            
            // Intersection (AND operation)
            leftResult.and(rightResult);
            return leftResult;
        }
    }
    
    public static class OR extends LOGIC {
        private static final long serialVersionUID = 1L;
        final LOGIC left; final LOGIC right;
        public OR(LOGIC left, LOGIC right) { 
            this.left = left; this.right = right; 
        }
        
        @Override 
        public BitSet evaluate(DataFrame13 df, String primaryColumn, int segmentIndex) {
            BitSet leftResult = left.evaluate(df, primaryColumn, segmentIndex);
            BitSet rightResult = right.evaluate(df, primaryColumn, segmentIndex);
            
            // Union (OR operation)
            leftResult.or(rightResult);
            return leftResult;
        }
    }

    public static class NOT extends LOGIC {
        private static final long serialVersionUID = 1L;
        final LOGIC inner;
        public NOT(LOGIC inner) { this.inner = inner; }
        
        @Override 
        public BitSet evaluate(DataFrame13 df, String primaryColumn, int segmentIndex) {
            BitSet innerResult = inner.evaluate(df, primaryColumn, segmentIndex);
            
            Column c = df.columns.get(primaryColumn);
            if (c == null) throw new IllegalArgumentException("Primary column not found for NOT evaluation.");
            SegmentHeader header = c.segmentHeaders[segmentIndex];
            
            BitSet fullSet = new BitSet(header.count);
            fullSet.set(0, header.count);
            
            fullSet.andNot(innerResult);
            return fullSet;
        }
    }
    // --- END PREDICATE CLASSES ---

    public static class IntList implements Serializable {
        private static final long serialVersionUID = 1L;
        private int[] data; private int size = 0; private static final int DEFAULT_CAPACITY = 16;
        public IntList() { this.data = new int[DEFAULT_CAPACITY]; }
        public IntList(int initialCapacity) { this.data = new int[initialCapacity]; }
        public void add(int val) {
            if (size == data.length) { data = Arrays.copyOf(data, data.length * 2); }
            data[size++] = val;
        }
        public int size() { return size; }
        public int get(int index) { 
             if (index < 0 || index >= size) throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size);
             return data[index]; 
        }
        public void set(int index, int val) { data[index] = val; }
        public void clear() { size = 0; }
    }

    static class BitReader {
        private final ByteBuffer data; private int bytePos = 0, bitPos = 0;
        BitReader(ByteBuffer data) { this.data = data; this.data.position(0); }
        
        // --- FIXED BITPACKING LOGIC (LSB-FIRST DECODING) ---
        int readBits(int w) {
            if (w == 0) return 0;

            int result = 0;
            int remaining = w;
            int resultBitPos = 0; 

            while (remaining > 0) {
                if (bytePos >= data.limit()) return 0;

                int currentByte = data.get(bytePos) & 0xFF;
                int availableBits = 8 - bitPos;
                int takeBits = Math.min(remaining, availableBits);

                int extracted = (currentByte >> bitPos) & ((1 << takeBits) - 1);
                
                result |= (extracted << resultBitPos); 
                
                resultBitPos += takeBits; 
                bitPos += takeBits;
                remaining -= takeBits;

                if (bitPos == 8) {
                    bytePos++;
                    bitPos = 0;
                }
            }
            return result;
        }
    }
 
    // --- Column must be static and Serializable ---
    static class Column implements Serializable {
        private static final long serialVersionUID = 1L;
        final String name;
        
        final Map<Object,Integer> dictMap = new LinkedHashMap<>();
        final List<Object> dict = new ArrayList<>();
        int[] numericDictValues;
        
        Class<?> inferredType = null; 
        transient final IntList rawIds = new IntList(); 
        
        SegmentHeader[] segmentHeaders; 
        Map<Integer, List<Integer>> preBuiltJoinIndex = null; 
        
        transient MappedByteBuffer mapped = null; 
        transient Path filePath = null;

        Column(String name) { 
            this.name = name; 
            this.numericDictValues = new int[0]; 
            this.inferredType = String.class; // Default to String
        }
        public int[] getDictValues() { return numericDictValues; }
        
        int idFor(Object v) {
            if (v == null) v = "__NULL__";
            Integer id = dictMap.get(v);
            if (id != null) return id;
            id = dict.size();
            dictMap.put(v, id);
            dict.add(v);
            return id;
        }

        Integer dictIdFor(Object v) {
            if (v == null) v = "__NULL__";
            return dictMap.get(v);
        }

        void add(Object v) { rawIds.add(idFor(v)); }
        
        boolean isNumericDict() { 
            if (inferredType != null) {
                return inferredType.equals(Integer.class) || inferredType.equals(Double.class);
            }
            return !dict.isEmpty() && dict.stream().anyMatch(o -> o instanceof Number);
        }
        void clearRaw() { rawIds.clear(); }

        /**
         * CRITICAL FIX: Ensures String entries in the dictionary are converted to 
         * Numbers if the column is numeric, so they can be stored as primitives.
         */
        void finalizeNumericDict() {
            if (isNumericDict()) {
                int[] values = new int[dict.size()];
                for (int i = 0; i < dict.size(); i++) {
                    Object dictValue = dict.get(i);
                    int numericVal = 0;
                    
                    if (dictValue instanceof Number) {
                        // Case 1: Value is already a Number (Integer, Double, etc.)
                        if (dictValue instanceof Double) {
                             numericVal = (int) Math.round(((Double) dictValue)); 
                        } else {
                            numericVal = ((Number) dictValue).intValue();
                        }
                    } else if (dictValue instanceof String) {
                        // Case 2 (The Fix): Value is a String, but the type is numeric. Convert it.
                         try {
                             if (inferredType.equals(Integer.class)) {
                                 numericVal = Integer.parseInt((String) dictValue);
                             } else if (inferredType.equals(Double.class)) {
                                 numericVal = (int) Math.round(Double.parseDouble((String) dictValue));
                             }
                         } catch (NumberFormatException e) {
                             // Fall back to 0 if parsing fails
                             numericVal = 0; 
                         }
                    } else {
                        // Case 3: Other non-numeric/null values
                        numericVal = 0; 
                    }
                    
                    values[i] = numericVal;
                }
                this.numericDictValues = values;
            }
        }
        
        Segment buildSegment(int from, int to) {
            final int n = to - from; Segment seg = new Segment();
            seg.count = n; seg.enc = Encoding.BITPACK; 
            
            BitmapIndex index = new BitmapIndex();
            long sum = 0; int min = Integer.MAX_VALUE, max = Integer.MIN_VALUE;
            
            int maxDictId = 0;
            for(int i = from; i < to; i++) {
                int dictId = rawIds.get(i);
                index.set(dictId, i - from);
                seg.presentIds.add(dictId); 
                maxDictId = Math.max(maxDictId, dictId);
                
                if (isNumericDict() && dictId < numericDictValues.length) {
                    int val = numericDictValues[dictId];
                    sum += val;
                    if (val < min) min = val; if (val > max) max = val;
                }
            }

            seg.sum = isNumericDict() ? sum : 0;
            seg.min = isNumericDict() ? (min == Integer.MAX_VALUE ? 0 : min) : 0;
            seg.max = isNumericDict() ? (max == Integer.MIN_VALUE ? 0 : max) : 0;
            seg.bitmapIndex = index;

            int totalBits = 0;
            if (n > 0) {
                seg.bits = maxDictId == 0 ? 1 : 32 - Integer.numberOfLeadingZeros(maxDictId);
                totalBits = seg.bits * n;
            } else {
                 seg.bits = 0;
            }
            
            seg.fileLength = (totalBits + 7) / 8; 
            
            if (n == 0) seg.fileLength = 0;
            
            return seg;
        }
    }

    private static class SerializableState implements Serializable {
        private static final long serialVersionUID = 1L;
        
        final Map<String, Column> columns;
        final int totalRows;
        final int segmentSize;
        final Map<String, Integer> globalRowIndexToSegIndex;

        SerializableState(DataFrame13 df) {
            this.columns = df.columns;
            this.totalRows = df.totalRows;
            this.segmentSize = df.segmentSize;
            this.globalRowIndexToSegIndex = df.globalRowIndexToSegIndex;
        }
    }


    /* ---------------------------
        DataFrame13 Core
    ---------------------------- */
    private final Map<String, Column> columns = new LinkedHashMap<>();
    private final int segmentSize;
    private boolean schemaInited = false;
    private int totalRows = 0;
    private final String engineName = "DataFrame13"; 
    
    private final Map<String, Integer> globalRowIndexToSegIndex = new HashMap<>(); 
    private final IntList rowIndexView; 

    public DataFrame13(int segmentSize) { 
        this.segmentSize = Math.max(1, segmentSize); 
        this.rowIndexView = new IntList(); 
    }

    // Existing CONSTRUCTOR for filtering (WHERE)
    private DataFrame13(DataFrame13 sourceDf, IntList view) {
        this.segmentSize = sourceDf.segmentSize;
        this.columns.putAll(sourceDf.columns);
        this.globalRowIndexToSegIndex.putAll(sourceDf.globalRowIndexToSegIndex);
        
        this.rowIndexView = view; 
        this.totalRows = view.size(); 
        this.schemaInited = true; 
        
        for (Column c : this.columns.values()) {
            if (c.filePath != null) {
                try (FileChannel rfc = FileChannel.open(c.filePath, StandardOpenOption.READ)) {
                    long totalLength = c.segmentHeaders.length > 0 ? (c.segmentHeaders[c.segmentHeaders.length - 1].fileOffset + c.segmentHeaders[c.segmentHeaders.length - 1].fileLength) : 0;
                    c.mapped = rfc.map(FileChannel.MapMode.READ_ONLY, 0, totalLength);
                    c.mapped.order(ByteOrder.BIG_ENDIAN); 
                } catch (IOException e) {
                      System.err.println("Warning: Could not re-map file for view: " + c.filePath);
                }
            }
        }
    }
    
    // NEW CONSTRUCTOR for column projection (SELECT)
    private DataFrame13(DataFrame13 sourceDf, Map<String, Column> selectedColumns) {
        // Inherit execution settings and view indices (filter)
        this.segmentSize = sourceDf.segmentSize;
        this.globalRowIndexToSegIndex.putAll(sourceDf.globalRowIndexToSegIndex);
        this.rowIndexView = sourceDf.rowIndexView;
        this.totalRows = sourceDf.totalRows;
        this.schemaInited = true; 

        // Inherit ONLY the selected column objects
        this.columns.putAll(selectedColumns);
        
        // Mapped buffers are shared and already initialized in sourceDf.
    }
    
    /**
     * Adds a row of data represented by a Map (for dynamic schema loading).
     */
    public void add(Map<String, Object> rowData) {
        if (!schemaInited) {
            // Initialize schema based on the keys of the first row
            for(String name : rowData.keySet()) {
                 columns.put(name, new Column(name));
            }
            schemaInited = true;
        }

        for (Map.Entry<String, Object> entry : rowData.entrySet()) {
            String colName = entry.getKey();
            Object v = entry.getValue();
            
            Column c = columns.get(colName);
            if (c != null) {
                c.add(v);
            }
        }
        totalRows++;
    }


    public void finalizeInMemory() {
        for (Column c : columns.values()) { c.finalizeNumericDict(); } 
        Map<String, List<Segment>> tempSegments = new HashMap<>();

        for (Column c : columns.values()) {
            List<Segment> segs = new ArrayList<>();
            int n = c.rawIds.size();
            int globalRow = 0;
            for (int i = 0; i < n; i += segmentSize) {
                int to = Math.min(n, i + segmentSize);
                Segment seg = c.buildSegment(i, to);
                segs.add(seg);
                
                for(int k=0; k < seg.count; k++) {
                    globalRowIndexToSegIndex.put(c.name + ":" + globalRow++, segs.size() - 1); 
                }
            }
            tempSegments.put(c.name, segs);
            c.clearRaw();
        }
        
        for (Column c : columns.values()) {
            List<Segment> segs = tempSegments.get(c.name);
            // FIX: Use .toArray(new SegmentHeader[0]) for broad Java compatibility
            c.segmentHeaders = segs.stream()
                .map(SegmentHeader::new)
                .collect(Collectors.toList())
                .toArray(new SegmentHeader[0]);
        }
    }

    public List<Path> finalizeStore(Path storageDir, boolean buildJoinIndex) throws IOException {
        Files.createDirectories(storageDir);
        List<Path> writtenFiles = new ArrayList<>();

        for (Column c : columns.values()) { c.finalizeNumericDict(); } 

        Map<String, List<Segment>> tempSegments = new HashMap<>();
        long currentOffset = 0;

        for (Column c : columns.values()) {
            if (buildJoinIndex && c.preBuiltJoinIndex == null) {
                 c.preBuiltJoinIndex = new HashMap<>();
                 for(int i = 0; i < c.rawIds.size(); i++) {
                     c.preBuiltJoinIndex.computeIfAbsent(c.rawIds.get(i), k -> new ArrayList<>()).add(i);
                 }
            }
            
            List<Segment> segs = new ArrayList<>();
            int n = c.rawIds.size();
            currentOffset = 0;
            Path colFile = storageDir.resolve(c.name + ".col");
            
            // Writing segment data
            try (FileChannel fc = FileChannel.open(colFile, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
                int globalRow = 0;
                for (int i = 0; i < n; i += segmentSize) {
                    int to = Math.min(n, i + segmentSize);
                    Segment seg = c.buildSegment(i, to);
                    
                    // --- WRITE ENCODED DATA TO FILE ---
                    ByteBuffer segmentData = encodeSegmentToBuffer(c, i, to, seg);
                    seg.fileOffset = currentOffset;
                    seg.fileLength = segmentData.limit();
                    currentOffset += seg.fileLength;
                    
                    fc.write(segmentData);
                    
                    segs.add(seg);
                    
                    for(int k=0; k < seg.count; k++) {
                        globalRowIndexToSegIndex.put(c.name + ":" + globalRow++, segs.size() - 1); 
                    }
                }
                fc.force(true);
            }

            try (FileChannel rfc = FileChannel.open(colFile, StandardOpenOption.READ)) {
                c.mapped = rfc.map(FileChannel.MapMode.READ_ONLY, 0, currentOffset);
                c.mapped.order(ByteOrder.BIG_ENDIAN); 
                c.filePath = colFile;
            }
            
            tempSegments.put(c.name, segs);
            c.clearRaw();
            writtenFiles.add(colFile);
        }
        
        for (Column c : columns.values()) {
            List<Segment> segs = tempSegments.get(c.name);
            // FIX: Use .toArray(new SegmentHeader[0]) for broad Java compatibility
            c.segmentHeaders = segs.stream()
                .map(SegmentHeader::new)
                .collect(Collectors.toList())
                .toArray(new SegmentHeader[0]);
        }
        
        return writtenFiles;
    }

    private ByteBuffer encodeSegmentToBuffer(Column c, int from, int to, Segment seg) {
        final int n = to - from;
        ByteBuffer buffer = ByteBuffer.allocate(seg.fileLength);
        buffer.order(ByteOrder.BIG_ENDIAN);

        if (seg.enc == Encoding.BITPACK) {
            int currentByte = 0;
            int bitCount = 0;
            int bits = seg.bits;

            for (int i = from; i < to; i++) {
                int dictId = c.rawIds.get(i);
                
                for (int b = 0; b < bits; b++) { 
                    int bit = (dictId >> b) & 1; 
                    
                    currentByte |= (bit << bitCount);
                    bitCount++;

                    if (bitCount == 8) {
                        buffer.put((byte) currentByte);
                        currentByte = 0;
                        bitCount = 0;
                    }
                }
            }
            // Flush remaining bits
            if (bitCount > 0) {
                buffer.put((byte) currentByte);
            }
        }
        buffer.flip();
        return buffer;
    }
    
    public void save(Path storageDir) throws IOException {
        Path metadataPath = storageDir.resolve(METADATA_FILE);
        SerializableState state = new SerializableState(this);
        
        try (ObjectOutputStream oos = new ObjectOutputStream(
                new BufferedOutputStream(Files.newOutputStream(metadataPath)))) {
            oos.writeObject(state);
            System.out.println("INFO: DataFrame metadata saved successfully to " + metadataPath);
        }
    }

    public static DataFrame13 FROM(Path storageDir) throws IOException, ClassNotFoundException {
        Path metadataPath = storageDir.resolve(METADATA_FILE);
        if (!Files.exists(metadataPath)) {
            throw new FileNotFoundException("Metadata file not found at " + metadataPath);
        }
        
        SerializableState state;
        try (ObjectInputStream ois = new ObjectInputStream(
                new BufferedInputStream(Files.newInputStream(metadataPath)))) {
            state = (SerializableState) ois.readObject();
            System.out.println("INFO: DataFrame metadata loaded successfully.");
        }

        DataFrame13 df = new DataFrame13(state.segmentSize);
        df.columns.putAll(state.columns);
        df.totalRows = state.totalRows;
        df.globalRowIndexToSegIndex.putAll(state.globalRowIndexToSegIndex);
        
        // Re-map column files
        for (Column c : df.columns.values()) {
            Path colFile = storageDir.resolve(c.name + ".col");
            
            long totalLength = 0;
            if (c.segmentHeaders != null && c.segmentHeaders.length > 0) {
                 SegmentHeader lastHeader = c.segmentHeaders[c.segmentHeaders.length - 1];
                 totalLength = lastHeader.fileOffset + lastHeader.fileLength;
            }

            try (FileChannel rfc = FileChannel.open(colFile, StandardOpenOption.READ)) {
                c.mapped = rfc.map(FileChannel.MapMode.READ_ONLY, 0, totalLength);
                c.mapped.order(ByteOrder.BIG_ENDIAN); 
                c.filePath = colFile;
            } catch (NoSuchFileException e) {
                System.err.println("ERROR: Column data file not found: " + colFile);
                throw e;
            }
        }

        if (!df.columns.isEmpty()) {
             df.schemaInited = true;
        }

        return df;
    }
    
    public void close() {
        queryThreadPool.shutdownNow();
        for (Column c : columns.values()) { c.mapped = null; } 
        System.out.println("INFO: MappedByteBuffer references dropped. Cleanup relies on JVM GC.");
    }
    
    private ByteBuffer getSegmentBufferSlice(Column c, SegmentHeader header) {
        if (c.mapped == null || header.fileLength == 0) return ByteBuffer.allocate(0);
        ByteBuffer slice = c.mapped.duplicate();
        slice.position((int) header.fileOffset);
        slice.limit((int) header.fileOffset + header.fileLength);
        slice.order(ByteOrder.BIG_ENDIAN); 
        return slice.slice();
    }
    
    // --- IMPLEMENTED DECODING LOGIC ---
    private int[] decodeSegmentToIds(Column c, SegmentHeader header) {
        
        int[] arr = new int[header.count];
        if (header.count == 0) return arr;
        
        ByteBuffer segmentSlice = getSegmentBufferSlice(c, header);
        
        if (header.enc == Encoding.CONSTANT) {
             // Assuming CONSTANT stores one 4-byte ID
             int dictId = segmentSlice.getInt(0);
             Arrays.fill(arr, dictId);
             return arr;
        } else if (header.enc == Encoding.RLE) {
            // Placeholder RLE logic
            int outputIndex = 0;
            while (outputIndex < header.count) {
                // Check if enough bytes remain for both ID and length (8 bytes total)
                if (segmentSlice.remaining() < 8) break; 
                
                int dictId = segmentSlice.getInt();
                int runLength = segmentSlice.getInt();
                
                for (int k = 0; k < runLength && outputIndex < header.count; k++) {
                    arr[outputIndex++] = dictId;
                }
            }
            return arr;
            
        } else if (header.enc == Encoding.BITPACK) {
            BitReader reader = new BitReader(segmentSlice);
            for (int i = 0; i < header.count; i++) {
                arr[i] = reader.readBits(header.bits); 
            }
            return arr;
        }

        throw new UnsupportedOperationException("Unsupported segment encoding: " + header.enc);
    }
    
    private int getDictIdForGlobalIndex(String colName, int globalIndex) {
        Column c = columns.get(colName);
        if (c == null) {
            throw new IllegalArgumentException("Column '" + colName + "' not found.");
        }
        
        Integer segIndex = globalRowIndexToSegIndex.get(colName + ":" + globalIndex);
        if (segIndex == null) {
             // If a row exists in the global totalRows but isn't indexed, it's a structural error
             throw new IllegalStateException("Missing segment index for global row: " + globalIndex + " in column " + colName);
        }
        
        SegmentHeader header = c.segmentHeaders[segIndex];
        
        int globalStartRow = 0;
        // Calculate the starting global row for the segment
        for(int i = 0; i < segIndex; i++) { globalStartRow += c.segmentHeaders[i].count; }
        int rowLocalIndex = globalIndex - globalStartRow;

        int[] segIds = decodeSegmentToIds(c, header);
        
        // FINAL BUGFIX: Ensure we are within the bounds of the decoded array
        if (rowLocalIndex < 0 || rowLocalIndex >= segIds.length) {
             throw new IndexOutOfBoundsException("Local index mismatch: " + rowLocalIndex + " for segment size " + segIds.length);
        }
        
        return segIds[rowLocalIndex];
    }
    // -------------------------------------------------------------
    
    /**
     * Core method to evaluate a single ValuePredicate against a segment.
     */
    BitSet evaluateSegment(COMPARE p, int targetSegmentIndex) {
        Column c = columns.get(p.column);
        if (c == null) throw new IllegalArgumentException("Column '" + p.column + "' not found.");

        BitSet segmentMatches = new BitSet();
        
        SegmentHeader header = c.segmentHeaders[targetSegmentIndex];
        
        // 1. Prepare Filter Value and Dictionary IDs
        Object filterValue = p.value;
        Integer filterDictId = c.dictIdFor(filterValue);
        
        // If the column is numeric and filter value is a Number
        if (c.isNumericDict() && filterValue instanceof Number) {
            int numericFilterValue = ((Number) filterValue).intValue();

            // 2. Pruning (Min/Max) - Check if the entire segment can be excluded
            if ( (p.op == OPERATOR.LT && numericFilterValue <= header.min) ||
                 (p.op == OPERATOR.LE && numericFilterValue < header.min) ||
                 (p.op == OPERATOR.GT && numericFilterValue >= header.max) ||
                 (p.op == OPERATOR.GE && numericFilterValue <= header.max) ) {
                return segmentMatches; // Empty BitSet
            }
            
            // 3. Full Scan for all operators (necessary for numeric range checks)
            int[] segmentIds = decodeSegmentToIds(c, header);
            for (int i = 0; i < segmentIds.length; i++) {
                int dictId = segmentIds[i];
                // Check bounds before accessing numericDictValues
                int actualValue = (dictId < c.numericDictValues.length) ? c.numericDictValues[dictId] : Integer.MIN_VALUE;
                
                boolean match = false;
                switch (p.op) {
                    case LT: match = actualValue < numericFilterValue; break;
                    case LE: match = actualValue <= numericFilterValue; break;
                    case GT: match = actualValue > numericFilterValue; break;
                    case GE: match = actualValue >= numericFilterValue; break;
                    case EQ: match = actualValue == numericFilterValue; break;
                    case NE: match = actualValue != numericFilterValue; break;
                    default: break; 
                }
                if (match) segmentMatches.set(i);
            }
            
        } else if (p.op == OPERATOR.EQ) {
            // String/Object Equality using Bitmap Index
            if (filterDictId != null) {
                segmentMatches.or(header.bitmapIndex.get(filterDictId));
            }
        } else {
             // If non-numeric AND not EQ/NE, throw exception.
             throw new UnsupportedOperationException("Unsupported operator (" + p.op + ") for non-numeric column '" + p.column + "'.");
        }

        return segmentMatches;
    }

    /**
     * Recursively traverses the predicate tree to find all unique column names involved.
     */
    private void getInvolvedColumns(LOGIC p, Set<String> columns) {
        if (p instanceof COMPARE) {
            columns.add(((COMPARE) p).column);
        } else if (p instanceof AND) {
            getInvolvedColumns(((AND) p).left, columns);
            getInvolvedColumns(((AND) p).right, columns);
        } else if (p instanceof OR) {
            getInvolvedColumns(((OR) p).left, columns);
            getInvolvedColumns(((OR) p).right, columns);
        } else if (p instanceof NOT) {
            getInvolvedColumns(((NOT) p).inner, columns);
        }
    }


    /**
     * Public interface for complex filtering. Takes a Predicate AST root.
     */
    public DataFrame13 WHERE(LOGIC predicate) throws InterruptedException, ExecutionException {
        
        Set<String> involvedColumns = new HashSet<>();
        getInvolvedColumns(predicate, involvedColumns); 

        String primaryColumn = involvedColumns.stream().findFirst().orElse(null);
        if (primaryColumn == null) return new DataFrame13(this, new IntList()); 

        Column pCol = columns.get(primaryColumn);
        if (pCol.segmentHeaders == null) throw new IllegalStateException("DataFrame must be finalized before filtering.");

        List<Callable<Pair<Integer, BitSet>>> tasks = new ArrayList<>();
        // Use the view of the current DF, if active
        IntList currentViewIndices = (this.rowIndexView.size() > 0) ? this.rowIndexView : null;

        for (int si = 0; si < pCol.segmentHeaders.length; si++) {
            final int segmentIndex = si;
            
            tasks.add(() -> {
                // Recursively evaluate the predicate on the segment, resulting in local matches
                BitSet localMatches = predicate.evaluate(this, primaryColumn, segmentIndex);
                
                // Calculate the global starting row index for this segment
                int globalStartRow = 0;
                for(int i = 0; i < segmentIndex; i++) {
                    globalStartRow += pCol.segmentHeaders[i].count;
                }
                // Return the starting global row and the BitSet of local matches
                return new Pair<>(globalStartRow, localMatches);
            });
        }
        
        List<Future<Pair<Integer, BitSet>>> futures = queryThreadPool.invokeAll(tasks);
        IntList newGlobalIndices = new IntList();
        
        // This is the global mapping of row index to its position in the view.
        // Used for efficient filter chaining when currentViewIndices is large.
        Set<Integer> currentViewSet = null; 
        if (currentViewIndices != null) {
            currentViewSet = new HashSet<>(currentViewIndices.size());
            for(int i = 0; i < currentViewIndices.size(); i++) {
                currentViewSet.add(currentViewIndices.get(i));
            }
        }
        
        for (Future<Pair<Integer, BitSet>> future : futures) {
            Pair<Integer, BitSet> result = future.get();
            int globalStartRow = result.left;
            BitSet localMatches = result.right;
            
            for (int i = localMatches.nextSetBit(0); i >= 0; i = localMatches.nextSetBit(i + 1)) {
                int globalIndex = globalStartRow + i;
                
                if (currentViewSet == null) {
                    // No prior filter, simply add the global index
                    newGlobalIndices.add(globalIndex);
                } else {
                    // Filter chaining: check if the global index is part of the current view
                    if (currentViewSet.contains(globalIndex)) {
                        newGlobalIndices.add(globalIndex);
                    }
                }
                if (i == Integer.MAX_VALUE) break; // Check for overflow when using int
            }
        }
        
        // Return a new DataFrame representing the filtered subset
        // We sort the indices to maintain scan order for future operations
        newGlobalIndices.data = Arrays.copyOf(newGlobalIndices.data, newGlobalIndices.size);
        Arrays.sort(newGlobalIndices.data, 0, newGlobalIndices.size); // Only sort up to size
        
        return new DataFrame13(this, newGlobalIndices);
    }
    
    /**
     * Creates a new DataFrame view containing only the specified columns, 
     * preserving the existing row filter (rowIndexView).
     * This is a projection operation.
     */
    public DataFrame13 SELECT(String... columnNames) {
        if (columnNames == null || columnNames.length == 0) {
            // If no columns are passed, SELECT all current columns
            return new DataFrame13(this, this.columns);
        }

        Map<String, Column> selectedColumns = new LinkedHashMap<>();
        
        for (String name : columnNames) {
            Column c = this.columns.get(name);
            if (c == null) {
                throw new IllegalArgumentException("Column '" + name + "' not found in DataFrame.");
            }
            // Projecting columns means adding the existing column objects 
            // to the new DataFrame's column map. They share the same underlying data/mappings.
            selectedColumns.put(name, c);
        }

        // Use the new internal constructor to create the projected DataFrame.
        return new DataFrame13(this, selectedColumns);
    }

    public void TOCSV(Path outputPath, String[] columnNames, String delimiter) throws IOException {
        
        if (columnNames == null || columnNames.length == 0) {
            columnNames = this.columns.keySet().toArray(new String[0]);
        }
        
        for (String name : columnNames) {
            if (!this.columns.containsKey(name)) {
                throw new IllegalArgumentException("Column '" + name + "' not found in DataFrame.");
            }
        }

        try (BufferedWriter writer = Files.newBufferedWriter(outputPath, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
            
            // 1. Write Header Row
            writer.write(String.join(delimiter, columnNames));
            writer.newLine();

            // Determine which indices to iterate over
            IntList indicesToUse = this.rowIndexView.size() > 0 ? this.rowIndexView : new IntList(this.totalRows);
            if (this.rowIndexView.size() == 0 && this.totalRows > 0) {
                for (int i = 0; i < this.totalRows; i++) {
                    indicesToUse.add(i);
                }
            }

            // 2. Write Data Rows
            for (int i = 0; i < indicesToUse.size(); i++) {
                // IMPORTANT: We use the View Index (i) for getRowData
                Map<String, Object> rowData = getRowData(i, columnNames); 
                
                StringBuilder sb = new StringBuilder();
                boolean first = true;
                
                for (String colName : columnNames) {
                    if (!first) {
                        sb.append(delimiter);
                    }
                    Object value = rowData.get(colName);
                    
                    String valueString = value == null ? "" : value.toString();
                    
                    if (value instanceof String && valueString.contains(delimiter)) {
                         sb.append('"').append(valueString.replace("\"", "\"\"")).append('"');
                    } else {
                         sb.append(valueString);
                    }
                    
                    first = false;
                }
                writer.write(sb.toString());
                writer.newLine();
            }
            
            System.out.println("INFO: Successfully saved " + indicesToUse.size() + " rows to CSV at " + outputPath);
        }
    }


    /* ======================================================
       CSV LOADING WITH DYNAMIC SCHEMA INFERENCE
       ====================================================== */
    
    /**
     * Loads a DataFrame from a CSV file, inferring the schema (column names and types)
     * by inspecting the data.
     */
    public static DataFrame13 FROMCSV(Path csvPath, String delimiter, int segmentSize) 
            throws IOException {
        
        DataFrame13 df = new DataFrame13(segmentSize);
        List<String> lines = Files.readAllLines(csvPath);
        if (lines.isEmpty()) {
            return df;
        }

        // --- 1. Get Headers and Initialize Columns ---
        String[] headers = lines.get(0).split(delimiter);
        Map<String, Class<?>> inferredTypes = new LinkedHashMap<>();
        
        for (String header : headers) {
            String name = header.trim();
            df.columns.put(name, new Column(name));
            inferredTypes.put(name, String.class); // Default to String (most general)
        }

        // --- 2. Pass 2: Infer Data Types (Simplified Full-File Scan) ---
        for (int i = 1; i < lines.size(); i++) {
            String[] tokens = lines.get(i).split(delimiter);
            
            for (int colIndex = 0; colIndex < headers.length; colIndex++) {
                if (colIndex >= tokens.length) continue;
                String headerName = headers[colIndex].trim();
                String stringValue = tokens[colIndex].trim();
                
                Class<?> currentInferredType = inferredTypes.get(headerName);
                
                if (stringValue.isEmpty()) {
                    continue; 
                }
                
                Class<?> nextType = currentInferredType;
                
                // If it's already a string, we cannot narrow it down.
                if (currentInferredType.equals(String.class)) {
                    continue;
                }
                
                try {
                    // Try Integer
                    Integer.parseInt(stringValue);
                    
                    if (currentInferredType.equals(Double.class)) { 
                        // If it was already Double, keep it Double, as this integer value 
                        // could be a float (e.g., 5.0) in other rows.
                    } else {
                         // Promote from default String/null or Integer (stays Integer)
                         nextType = Integer.class;
                    }
                    
                } catch (NumberFormatException e1) {
                    try {
                        // Try Double
                        Double.parseDouble(stringValue);
                        // Promote to Double (from Integer or default String/null)
                        nextType = Double.class;
                        
                    } catch (NumberFormatException e2) {
                        // If it failed Double, it must be String (most general type)
                        nextType = String.class;
                    }
                }
                
                // Set the determined type (most general takes precedence)
                inferredTypes.put(headerName, nextType);
            }
        }
        
        // --- 3. Pass 3: Load Data using Inferred Types (Stores data as the appropriate object) ---
        for (int i = 1; i < lines.size(); i++) {
            String[] tokens = lines.get(i).split(delimiter);
            
            // Prepare a map representing the row
            Map<String, Object> rowData = new LinkedHashMap<>();
            
            for (int colIndex = 0; colIndex < headers.length; colIndex++) {
                if (colIndex >= tokens.length) continue;

                String headerName = headers[colIndex].trim();
                String stringValue = tokens[colIndex].trim();
                Class<?> type = inferredTypes.get(headerName);
                Object typedValue = null;
                
                if (stringValue.isEmpty()) {
                    typedValue = null; 
                } else if (type.equals(String.class)) {
                    typedValue = stringValue;
                } else if (type.equals(Integer.class)) {
                    try {
                        // FIX: Store as String here to preserve the original format in the dictionary,
                        // and let finalizeNumericDict handle the conversion to primitive array.
                        // This fixes the issue where the Dict has to contain the string for persistence.
                        // But wait, the original logic requires storing the typed object if inferred type != String.
                        // Let's rely on the fix in finalizeNumericDict instead. Store typed object if possible.
                        typedValue = Integer.parseInt(stringValue);
                    } catch (NumberFormatException e) { typedValue = stringValue; }
                } else if (type.equals(Double.class)) {
                    try {
                        typedValue = Double.parseDouble(stringValue);
                    } catch (NumberFormatException e) { typedValue = stringValue; }
                } 
                
                rowData.put(headerName, typedValue);
            }
            
            // Add the row map to the DataFrame
            df.add(rowData);
        }

        // Apply inferred type finalization
        for (Map.Entry<String, Column> entry : df.columns.entrySet()) {
            entry.getValue().inferredType = inferredTypes.get(entry.getKey());
        }
        
        System.out.println("INFO: CSV loaded with inferred schema: " + inferredTypes);
        return df;
    }


    /* ======================================================
       JOIN PRIMITIVES 
       ====================================================== */
    public static MATCHED LEFTMATCH(DataFrame13 engineA, String colA, DataFrame13 engineB, String colB) {
        MATCHED result = new MATCHED();
        Column colBRef = engineB.columns.get(colB);
        
        // Safety check (same as INNERMATCH)
        if (colBRef == null || colBRef.preBuiltJoinIndex == null) {
            throw new IllegalStateException("Table B must have a pre-built join index for this fast join.");
        }
        
        IntList viewA = engineA.rowIndexView.size() > 0 ? engineA.rowIndexView : null;
        IntList viewB = engineB.rowIndexView.size() > 0 ? engineB.rowIndexView : null;
        
        Map<Integer, List<Integer>> indexMapB = colBRef.preBuiltJoinIndex;

        // --- LEFT JOIN KEY ADDITION: Track if a match was found for the current row A ---
        // The current viewIndexA acts as the index into the resulting view of the left table.

        // Iterate over the rows in engineA's current view
        for (int viewIndexA = 0; viewIndexA < engineA.totalRows; viewIndexA++) {
             int globalIndexA = viewA != null ? viewA.get(viewIndexA) : viewIndexA;
             int dictIdA = engineA.getDictIdForGlobalIndex(colA, globalIndexA); 
             
             List<Integer> matchingIndicesB = indexMapB.get(dictIdA); 
             
             boolean matchFound = false; // Flag to check if we need to output a 'null' match

             if (matchingIndicesB != null) {
                 final int currentViewIndexA = viewIndexA;
                 
                 for (int globalIndexB : matchingIndicesB) { 
                     int viewIndexB = -1;
                     
                     // Reuse binary search logic from INNERMATCH to check against viewB
                     if (viewB == null) {
                         viewIndexB = globalIndexB; 
                     } else {
                         int low = 0; int high = viewB.size() - 1;
                         while(low <= high) {
                             int mid = low + ((high - low) / 2);
                             if (viewB.get(mid) == globalIndexB) { viewIndexB = mid; break; } 
                             else if (viewB.get(mid) < globalIndexB) { low = mid + 1; } 
                             else { high = mid - 1; }
                         }
                     }
                     
                     if (viewIndexB != -1) {
                        result.matches.add(new Pair<>(currentViewIndexA, viewIndexB));
                        matchFound = true; // Match successfully added
                     }
                 }
             }
             
             // --- LEFT JOIN CORE LOGIC: Add row A even if no match was found (or added) ---
             if (!matchFound) {
                 // Left row (viewIndexA) is included, but right row index is set to -1 (null/empty)
                 result.matches.add(new Pair<>(viewIndexA, -1)); 
             }
        }
        return result;
    }
    
    public static MATCHED OUTERMATCH(DataFrame13 engineA, String colA, DataFrame13 engineB, String colB) {
        MATCHED result = new MATCHED();
        Column colBRef = engineB.columns.get(colB);
        
        if (colBRef == null || colBRef.preBuiltJoinIndex == null) {
            throw new IllegalStateException("Table B must have a pre-built join index for this fast join.");
        }
        
        IntList viewA = engineA.rowIndexView.size() > 0 ? engineA.rowIndexView : null;
        IntList viewB = engineB.rowIndexView.size() > 0 ? engineB.rowIndexView : null;
        
        Map<Integer, List<Integer>> indexMapB = colBRef.preBuiltJoinIndex;

        // --- OUTER JOIN KEY ADDITION: Track B rows that have been matched ---
        BitSet matchedBIndices = new BitSet(engineB.totalRows); 
        
        // --- PART 1: Find all A-to-B matches and B-matched rows (Same as INNERMATCH/LEFTMATCH) ---
        for (int viewIndexA = 0; viewIndexA < engineA.totalRows; viewIndexA++) {
             int globalIndexA = viewA != null ? viewA.get(viewIndexA) : viewIndexA;
             int dictIdA = engineA.getDictIdForGlobalIndex(colA, globalIndexA); 
             
             List<Integer> matchingIndicesB = indexMapB.get(dictIdA); 
             boolean matchFound = false;

             if (matchingIndicesB != null) {
                 final int currentViewIndexA = viewIndexA;
                 
                 for (int globalIndexB : matchingIndicesB) { 
                     int viewIndexB = -1;
                     
                     // Find viewIndexB using binary search (reuse from INNERMATCH/LEFTMATCH)
                     if (viewB == null) {
                         viewIndexB = globalIndexB; 
                     } else {
                         int low = 0; int high = viewB.size() - 1;
                         while(low <= high) {
                             int mid = low + ((high - low) / 2);
                             if (viewB.get(mid) == globalIndexB) { viewIndexB = mid; break; } 
                             else if (viewB.get(mid) < globalIndexB) { low = mid + 1; } 
                             else { high = mid - 1; }
                         }
                     }
                     
                     if (viewIndexB != -1) {
                        result.matches.add(new Pair<>(currentViewIndexA, viewIndexB));
                        matchedBIndices.set(viewIndexB); // Mark the right-side row as matched
                        matchFound = true; 
                     }
                 }
             }
             
             // --- Include non-matched A rows (Part of LEFT JOIN logic) ---
             if (!matchFound) {
                 result.matches.add(new Pair<>(viewIndexA, -1)); 
             }
        }
        
        // --- PART 2: Find all non-matched B rows and add them with A index = -1 ---
        
        int totalViewBRows = viewB != null ? viewB.size() : engineB.totalRows;

        for (int viewIndexB = 0; viewIndexB < totalViewBRows; viewIndexB++) {
            // If the BitSet is NOT set, this row in B was NOT matched by any row in A.
            if (!matchedBIndices.get(viewIndexB)) {
                // Add the non-matched row B with the left index set to -1 (null/empty)
                result.matches.add(new Pair<>(-1, viewIndexB));
            }
        }
        
        return result;
    }
    
    public static MATCHED INNERMATCH(DataFrame13 engineA, String colA, DataFrame13 engineB, String colB) {
        MATCHED result = new MATCHED();
        Column colBRef = engineB.columns.get(colB);
        if (colBRef == null || colBRef.preBuiltJoinIndex == null) {
            throw new IllegalStateException("Table B must have a pre-built join index for this fast join.");
        }
        
        IntList viewA = engineA.rowIndexView.size() > 0 ? engineA.rowIndexView : null;
        IntList viewB = engineB.rowIndexView.size() > 0 ? engineB.rowIndexView : null;
        
        Map<Integer, List<Integer>> indexMapB = colBRef.preBuiltJoinIndex;

        // Iterate over the rows in engineA's current view
        for (int viewIndexA = 0; viewIndexA < engineA.totalRows; viewIndexA++) {
             int globalIndexA = viewA != null ? viewA.get(viewIndexA) : viewIndexA;
             int dictIdA = engineA.getDictIdForGlobalIndex(colA, globalIndexA); 

             List<Integer> matchingIndicesB = indexMapB.get(dictIdA); 
             if (matchingIndicesB != null) {
                 final int currentViewIndexA = viewIndexA;
                 
                 matchingIndicesB.forEach(globalIndexB -> { 
                     int viewIndexB = -1;
                     if (viewB == null) {
                         // No filter on B, global index is the view index
                         viewIndexB = globalIndexB; 
                     } else {
                         // Must find the matching globalIndexB inside viewB's list
                         
                         int low = 0;
                         int high = viewB.size() - 1;
                         // Binary search for globalIndexB in the view list
                         while(low <= high) {
                             int mid = low + ((high - low) / 2);
                             if (viewB.get(mid) == globalIndexB) {
                                 viewIndexB = mid;
                                 break;
                             } else if (viewB.get(mid) < globalIndexB) {
                                 low = mid + 1;
                             } else {
                                 high = mid - 1;
                             }
                         }
                         
                     }
                     
                     if (viewIndexB != -1) {
                        result.matches.add(new Pair<>(currentViewIndexA, viewIndexB));
                     }
                 });
             }
        }
        return result;
    }
    
    /**
     * Creates a new DataFrame by materializing the join result.
     */
    public static DataFrame13 DFFROMJOIN(
            MATCHED result, 
            DataFrame13 dfA, 
            DataFrame13 dfB) {
            
        DataFrame13 dfJoined = new DataFrame13(dfA.segmentSize); 
        
        Map<String, Pair<DataFrame13, String>> columnMap = new LinkedHashMap<>(); 
        
        // Add columns from DF A
        for (String colName : dfA.columns.keySet()) {
            String joinedColName = "A." + colName;
            columnMap.put(joinedColName, new Pair<>(dfA, colName));
            dfJoined.columns.put(joinedColName, new Column(joinedColName));
            dfJoined.columns.get(joinedColName).inferredType = dfA.columns.get(colName).inferredType;
        }
        
        // Add columns from DF B
        for (String colName : dfB.columns.keySet()) {
            String joinedColName = "B." + colName;
            columnMap.put(joinedColName, new Pair<>(dfB, colName));
            dfJoined.columns.put(joinedColName, new Column(joinedColName));
            dfJoined.columns.get(joinedColName).inferredType = dfB.columns.get(colName).inferredType;
        }
        
        dfJoined.schemaInited = true;

        for (Pair<Integer, Integer> match : result.matches) {
            int viewIndexA = match.left;
            int viewIndexB = match.right;
            
            // Note: getRowData retrieves data based on the view index.
            // Retrieve all columns available in the respective DataFrames
            String[] colsA = dfA.columns.keySet().toArray(new String[0]);
            String[] colsB = dfB.columns.keySet().toArray(new String[0]);
            
            Map<String, Object> dataA = dfA.getRowData(viewIndexA, colsA);
            Map<String, Object> dataB = dfB.getRowData(viewIndexB, colsB);
            
            
            // Prepare the row for the joined DataFrame
            Map<String, Object> rowData = new LinkedHashMap<>();

            for (Map.Entry<String, Pair<DataFrame13, String>> entry : columnMap.entrySet()) {
                String joinedColName = entry.getKey();
                String originalColName = entry.getValue().right;
                DataFrame13 sourceDf = entry.getValue().left;
                
                Object value;
                
                if (sourceDf == dfA) {
                    value = dataA.get(originalColName);
                } else { // sourceDf == dfB
                    value = dataB.get(originalColName);
                }
                
                rowData.put(joinedColName, value);
            }
            
            dfJoined.add(rowData); // Use the map-based add
        }
        
        dfJoined.finalizeInMemory(); 
        
        return dfJoined;
    }

    /* ======================================================
       DISPLAY/EXPLAIN METHODS
       ====================================================== */
       
    public void explain() {
        System.out.println("Engine: " + engineName + " Rows: " + totalRows + " segmentSize=" + segmentSize + " Parallelism=" + PARALLELISM_LEVEL);
        System.out.println("Vector Chunk Size: " + VECTOR_CHUNK_SIZE);
        System.out.println("View Status: " + (this.rowIndexView.size() > 0 ? "Active (Showing " + this.rowIndexView.size() + " rows)" : "Base DF (Showing " + this.totalRows + " rows)"));
        
        for (Map.Entry<String,Column> e : columns.entrySet()) {
            Column c = e.getValue();
            String numericInfo = c.numericDictValues.length > 0 ? (c.numericDictValues.length + " values stored primitively") : "0"; // Changed N/A to 0 for clarity
            String joinIndexInfo = c.preBuiltJoinIndex != null ? " (JOIN INDEXED)" : "";
            
            System.out.println("Column: " + c.name + " Type=" + (c.inferredType != null ? c.inferredType.getSimpleName() : "UNKNOWN") + " dictSize=" + c.dict.size() + " segments=" + (c.segmentHeaders != null ? c.segmentHeaders.length : 0) + joinIndexInfo + " Numeric Primitives: " + (c.isNumericDict() ? c.numericDictValues.length : "N/A"));
            if (c.segmentHeaders != null && c.segmentHeaders.length > 0) {
                 SegmentHeader h = c.segmentHeaders[0];
                 System.out.println("  -> Sample Segment (0): Enc=" + h.enc + " Bits=" + h.bits + " Length=" + h.fileLength + " Offset=" + h.fileOffset);
            }
        }
    }

    public Map<String, Object> getRowData(int viewIndex, String... columnNames) {
        
        final int rowIndex;
        if (this.rowIndexView.size() > 0) {
            if (viewIndex >= this.rowIndexView.size()) {
                 throw new IndexOutOfBoundsException("View index out of bounds: " + viewIndex + " for view size " + this.rowIndexView.size());
            }
            // Get the global row index from the view map
            rowIndex = this.rowIndexView.get(viewIndex); 
        } else {
            // No view active, viewIndex IS the global index
            rowIndex = viewIndex; 
        }

        Map<String, Object> rowData = new LinkedHashMap<>();
        
        // Use the columns defined in the CURRENT (potentially SELECTed) DataFrame
        String[] effectiveColumns = columnNames;
        if (columnNames == null || columnNames.length == 0) {
             effectiveColumns = this.columns.keySet().toArray(new String[0]);
        }
        
        for (String colName : effectiveColumns) {
            Column c = columns.get(colName);
            if (c == null) {
                rowData.put(colName, "N/A"); // Should not happen if SELECT was used correctly
                continue;
            }
            
            Integer segIndex = globalRowIndexToSegIndex.get(colName + ":" + rowIndex);
            
            if (segIndex != null) {
                 // No need to decode all segments in this method, rely on getDictIdForGlobalIndex
                 int dictId = getDictIdForGlobalIndex(colName, rowIndex); 
                 Object value = c.dict.get(dictId); 
                 rowData.put(colName, value);
            }
        }
        return rowData;
    }
    
    /**
     * Prints a specified number of rows from the current DataFrame view.
     * It shows all columns currently available in the projection.
     */
    public void SHOW(int rowsToShow) {
        
        // Use all columns available in the current projection/view
        String[] effectiveColumns = this.columns.keySet().toArray(new String[0]);
        
        // Use a generic table name
        String tableName = "Current DataFrame View"; 
        
        System.out.println("\n--- Table: " + tableName + " (Viewed Rows: " + totalRows + ", Columns: " + effectiveColumns.length + ") ---");
        
        Map<String, Integer> widths = new LinkedHashMap<>();
        for (String colName : effectiveColumns) { 
            widths.put(colName, Math.max(colName.length(), 10)); 
        }
        
        StringBuilder header = new StringBuilder();
        StringBuilder separator = new StringBuilder();
        for (String colName : effectiveColumns) {
            int w = widths.get(colName);
            header.append(String.format("| %-" + w + "s ", colName));
            separator.append(String.format("+-%s-", "-".repeat(w)));
        }
        header.append("|"); separator.append("+");
        
        System.out.println(separator);
        System.out.println(header);
        System.out.println(separator);
        
        int actualRowsToShow = Math.min(totalRows, rowsToShow); 
        
        if (totalRows > actualRowsToShow) {
             // System.out.println("--- Showing first " + actualRowsToShow + " rows of " + totalRows + " ---");
        }
        
        for (int i = 0; i < actualRowsToShow; i++) {
            Map<String, Object> rowData = getRowData(i, effectiveColumns);
            StringBuilder line = new StringBuilder();
            for (String colName : effectiveColumns) {
                int w = widths.get(colName);
                Object value = rowData.get(colName);
                String valueString = value != null ? value.toString() : "NULL"; 
                // Truncate if necessary for formatting
                String paddedValue = valueString.length() > w ? valueString.substring(0, w) : valueString;
                line.append(String.format("| %-" + w + "s ", paddedValue));
            }
            line.append("|");
            System.out.println(line);
        }
        
        System.out.println(separator);
    }
    
    
    /* ======================================================
      DATA ROW CLASSES (NOT USED FOR FROMCSV ANYMORE - Kept for JOIN/Test)
      ====================================================== */
    // Note: These classes are no longer used by FROMCSV, but are kept 
    // for the main method's data generation logic for context.
    public static class Customer { 
        public Integer customerID; public String name; 
        public Customer() {} 
        Customer(int id, String n) { customerID = id; name = n; }
    }

    public static class Order { 
        public Integer orderID; public Integer customerKey; public Double totalAmount; 
        public Order() {} 
        Order(int oid, int cid, double ta) { orderID = oid; customerKey = cid; totalAmount = ta; }
    }
    
    public static class JoinedRow {
        public int customerID; public String name; public int orderID; public int customerKey; public double totalAmount; 
        public JoinedRow() {} 
        public JoinedRow(int cId, String n, int oId, int cKey, double tA) {
            this.customerID = cId; this.name = n; this.orderID = oId;
            this.customerKey = cKey; this.totalAmount = tA;
        }
    }
    
    /* ======================================================
      MAIN METHOD (Testing)
      ====================================================== */
    public static void main(String[] args) throws Exception {
        //testdataframe
        Instant start = Instant.now();
        
    	Path outDir = Paths.get("./data_df13_output"); 
        Path pathA = outDir.resolve("A");
        Path pathB = outDir.resolve("B");
        Path csvA = outDir.resolve("customers.csv");
        Path csvB = outDir.resolve("orders.csv");
        Path filteredCsvPath = outDir.resolve("filtered_customers.csv");

        final int NUM_ROWS = 1000;
        final int NUM_UNIQUE_CUSTOMERS = 10; 
        
        // --- Cleanup and Setup ---
        if (Files.exists(outDir)) {
             try (DirectoryStream<Path> ds = Files.newDirectoryStream(outDir)) {
                 for (Path p : ds) {
                     if (Files.isDirectory(p)) {
                         try (DirectoryStream<Path> subDs = Files.newDirectoryStream(p)) {
                             for (Path subP : subDs) Files.deleteIfExists(subP);
                         }
                         Files.deleteIfExists(p);
                     } else {
                         Files.deleteIfExists(p);
                     }
                 }
             } catch (IOException ignored) {}
         }
         Files.createDirectories(outDir);
         
        // --- 1. POPULATE CSV DATA ---
        List<String> customerLines = new ArrayList<>();
        customerLines.add("customerID,name");
        for (int i = 0; i < NUM_ROWS; i++) {
            int customerID = (i % NUM_UNIQUE_CUSTOMERS) + 101;
            customerLines.add(customerID + ",Customer_" + i);
        }
        Files.write(csvA, customerLines);

        List<String> orderLines = new ArrayList<>();
        orderLines.add("orderID,customerKey,totalAmount");
        Random rand = new Random(42);
        for (int i = 0; i < NUM_ROWS; i++) {
            int orderID = 5000 + i;
            int customerKey = (i % NUM_UNIQUE_CUSTOMERS) + 101;
            double totalAmount = 10.0 + (rand.nextDouble() * 500.0);
            orderLines.add(orderID + "," + customerKey + "," + String.format("%.2f", totalAmount));
        }
        Files.write(csvB, orderLines);

        
        // --- 2. LOAD & SAVE DATA FRAMES (Now using Dynamic FROMCSV) ---
        System.out.println("\n--- 1. LOAD & STORE DATA FRAMES (Dynamic Schema Inference) ---");
        
        DataFrame13 dfA = DataFrame13.FROMCSV(csvA, ",", 20); 
        // Force the type for the specific test scenario where only int range works
        // This is necessary because FROMCSV might infer String if the first few rows are ambiguous.
        dfA.columns.get("customerID").inferredType = Integer.class; 
        dfA.finalizeStore(pathA, false); 
        dfA.save(pathA); 
        
        DataFrame13 dfB = DataFrame13.FROMCSV(csvB, ",", 20);
        dfB.columns.get("orderID").inferredType = Integer.class;
        dfB.columns.get("customerKey").inferredType = Integer.class;
        dfB.columns.get("totalAmount").inferredType = Double.class;
        dfB.finalizeStore(pathB, true); 
        dfB.save(pathB); 

        dfA.close(); 
        dfB.close();
        
        
        // --- 3. SIMULATING RESTART AND LOAD ---
        System.out.println("\n--- 2. SIMULATING RESTART AND LOAD ---");
        DataFrame13 dfA_loaded = DataFrame13.FROM(pathA);
        DataFrame13 dfB_loaded = DataFrame13.FROM(pathB);
        
        dfA_loaded.explain(); 
        
        
        // --- 4. COMPLEX FILTERING EXAMPLE ---
        System.out.println("\n--- 3. APPLYING COMPLEX FILTER PREDICATE (customerID = 101 OR customerID > 108) ---");
        
        // Predicate 1: customerID EQ 101
        COMPARE p1 = new COMPARE("customerID", 101, OPERATOR.EQ); 
        // Predicate 2: customerID GT 108 
        COMPARE p2 = new COMPARE("customerID", 108, OPERATOR.GT); 
        
        // Filter: (customerID = 101) OR (customerID > 108)
        LOGIC finalPredicate = new OR(p1, p2); 
        
        DataFrame13 dfA_complex_filtered = dfA_loaded.WHERE(finalPredicate);
        
        
        // --- 5. APPLYING SELECT/PROJECTION ---
        System.out.println("\n--- 4. SELECTING A SUBSET OF COLUMNS (df.SELECT(\"COL1\",\"COL2\")) ---");
        
        // Create a new DataFrame view df_s with only 'name' and 'customerID'
        DataFrame13 df_s = dfA_complex_filtered.SELECT("name", "customerID"); 
        
        System.out.println("Original DF A Filtered Row Count: " + dfA_complex_filtered.totalRows);
        System.out.println("Selected DF S Row Count: " + df_s.totalRows);
        
        // df_s.SHOW(13) prints 13 row (as requested)
        System.out.println("\n--- SHOW(13) of Selected View ---");
        df_s.SHOW(13); 

        
        // --- 6. SAVE THE FILTERED VIEW TO CSV ---
        System.out.println("\n--- 5. SAVING FILTERED VIEW TO CSV ---");
        String[] colsToExport = new String[]{"customerID", "name"};
        dfA_complex_filtered.TOCSV(filteredCsvPath, colsToExport, ",");
        
        
        // --- 7. JOINING THE FILTERED VIEW ---
        System.out.println("\n--- 6. JOINING THE FILTERED DF A WITH BASE DF B ---");
        String colA = "customerID";
        String colB = "customerKey";

        DataFrame13.MATCHED innerResult = INNERMATCH(dfA_complex_filtered, colA, dfB_loaded, colB);
        
        System.out.println("\nJOIN RESULT COUNT: " + innerResult.matches.size() + " matches found.");
        DataFrame13 dfJoined = DFFROMJOIN(innerResult, dfA_complex_filtered, dfB_loaded);
        dfJoined.SHOW(10);
        
        // --- Final Cleanup and Timing ---
        dfA_loaded.close();
        dfB_loaded.close();
        
        DataFrame13 demo1 = new DataFrame13(5);
     // 1. Prepare the data using the column names as keys and the data as values
        Map<String, Object> newOrderRow = new LinkedHashMap<>();
        newOrderRow.put("id1", 7001); 
        newOrderRow.put("id2", 111); 
        newOrderRow.put("id3", 75.50); 

        demo1.add(newOrderRow);
        
        //demo1.finalizeInMemory();
        demo1.SHOW(10);
        Path demo1path = outDir.resolve("demo1");
        demo1.finalizeStore(demo1path,true);
        demo1.save(demo1path);
        DataFrame13 demo2 = new DataFrame13(2);
     // 1. Prepare the data using the column names as keys and the data as values
        Map<String, Object> newOrderRow2 = new LinkedHashMap<>();
        newOrderRow2.put("id1", 7001); 
        newOrderRow2.put("id4", 134); 
        newOrderRow2.put("id5", 34.50); 
        Map<String, Object> newOrderRow3 = new LinkedHashMap<>();
        newOrderRow3.put("id1", 7034); 
        newOrderRow3.put("id4", 35); 
        newOrderRow3.put("id5", 5.45); 
       
        demo2.add(newOrderRow2);
        demo2.add(newOrderRow3);
       // demo2.finalizeInMemory();
        demo2.SHOW(10);
        Path demo2path = outDir.resolve("demo2");
        demo2.finalizeStore(demo2path,true);
        demo2.save(demo2path);
        DataFrame13 demo1r = DataFrame13.FROM(demo1path);
        DataFrame13 demo2r = DataFrame13.FROM(demo2path);
        demo1r.SHOW(2);
        demo2r.SHOW(2);
       DataFrame13.MATCHED keys =OUTERMATCH(demo2r,"id1",demo1r,"id1");
        DataFrame13 joined =DFFROMJOIN(keys,demo1r,demo2r);
        joined.SHOW(10);
        Instant end = Instant.now();

        Duration diff = Duration.between(start, end);

        System.out.println("\nTotal Execution Time: " + diff.toMillis() + " ms");
    }
}
