package com.externalsort;

import com.externalsort.helper.CsvFilelineStack;
import com.externalsort.helper.IStreamWrapper;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

public class SortedFilesMerger {
    private static final Logger LOG = LoggerFactory.getLogger(SortedFilesMerger.class);

    // this parameter to limit the max file merging in one process
    // to avoid open too many file descriptor.
    private static final int MAX_FILE_IN_ONE_MERGING = 256;


    /**
     * Merge a list of sorted files into one sorted file to avoid out of memory error
     * @param csvFiles input sorted CSV files
     * @param outputFile output file position
     * @param csvFormat he CSV Format you want, if no idea, use CSVFormat.DEFAULT
     * @param cmp comparator passed by user, to tell which key need to sort in CSVRecord
     * @param isDinstinct is the comparator key is distinct which means no two record are compare == 0
     *      if isDistinct == true, but the data is not distinct, it will ignore the duplicate key randomly
     * @param isAppend isAppend == true will append the file, otherwise, overwrite the file
     * @param wrapper this used for Stream Wrapper, eg. some one want to use AES Stream to protect file Stream
     * @return the file lines
     * @throws IOException
     */
    public static long mergeSortedCsvFiles(List<File> csvFiles, File outputFile, CSVFormat csvFormat,
                                           final Comparator<CSVRecord> cmp, boolean isDinstinct, boolean isAppend,
                                           IStreamWrapper wrapper) throws IOException {
        LOG.debug("mergeSortedCsvFiles file size {}", csvFiles.size());
        try {
            if (csvFiles.size() <= MAX_FILE_IN_ONE_MERGING) {
                List<InputStream> inputStreams = new ArrayList<>();
                for (File f : csvFiles) {
                    inputStreams.add(wrapper.wrap(new FileInputStream(f)));
                }
                if (!outputFile.exists()) {
                    outputFile.getParentFile().mkdirs();
                    outputFile.createNewFile();
                }
                OutputStream outputStream = wrapper.wrap(new FileOutputStream(outputFile, isAppend));

                return mergeSortedCsvFiles(inputStreams, outputStream, csvFormat, cmp, isDinstinct);
            }

            List<File> intermediateMerger = new ArrayList<>();
            File tmpFileFolder = csvFiles.get(0).getParentFile();
            for (int i = 0; i < csvFiles.size(); i += MAX_FILE_IN_ONE_MERGING) {
                List<File> tmp = new ArrayList<>();
                for (int j = i; j < Math.min(csvFiles.size(), i + MAX_FILE_IN_ONE_MERGING); j++) {
                    tmp.add(csvFiles.get(j));
                }
                File tempOutputFile = File.createTempFile("intermediateMerger", null, tmpFileFolder);
                mergeSortedCsvFiles(tmp, tempOutputFile, csvFormat, cmp, isDinstinct, isAppend, wrapper);
                intermediateMerger.add(tempOutputFile);
            }

            return mergeSortedCsvFiles(intermediateMerger, outputFile, csvFormat, cmp, isDinstinct, isAppend, wrapper);
        } finally {
            for (File f : csvFiles) {
                if (f.exists() && !f.delete()) {
                    LOG.warn("The file {} was not deleted", f.getName());
                }
            }
        }

    }

    /**
     * Merge a list of sorted inputStream into outputStream to avoid out of memory error
     * @param csvFiles input sorted CSV inputStream
     * @param outputStream output stream
     * @param csvFormat he CSV Format you want, if no idea, use CSVFormat.DEFAULT
     * @param cmp comparator passed by user, to tell which key need to sort in CSVRecord
     * @param isDinstinct is the comparator key is distinct which means no two record are compare == 0
     *      if isDistinct == true, but the data is not distinct, it will ignore the duplicate key randomly
     * @return the file lines
     * @throws IOException
     */
    public static long mergeSortedCsvFiles(List<InputStream> csvFiles, OutputStream outputStream,
                                           CSVFormat csvFormat, final Comparator<CSVRecord> cmp, boolean isDinstinct) throws IOException {
        try {
            List<CsvFilelineStack> csvList = new ArrayList<>();
            for (InputStream in : csvFiles) {
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(in));
                CSVParser parser = new CSVParser(bufferedReader, csvFormat);
                csvList.add(new CsvFilelineStack(parser));
            }
            BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(outputStream));

            return mergeSortedFiles(csvList, bufferedWriter, cmp, isDinstinct, csvFormat);
        } finally {
            for (InputStream in : csvFiles) {
                if (in != null) {
                    in.close();
                }
            }
            if (outputStream != null) {
                outputStream.close();
            }
        }
    }


    public static long mergeSortedCsvFiles(List<File> csvFiles, File outputFile, CSVFormat csvFormat,
                                           final Comparator<CSVRecord> cmp,
                                           boolean isDinstinct, boolean isAppend) throws IOException {
        return mergeSortedCsvFiles(csvFiles, outputFile, csvFormat, cmp, isDinstinct, isAppend, new IStreamWrapper() {});
    }

    private static long mergeSortedFiles(List<CsvFilelineStack> csvList, BufferedWriter bufferedWriter,
                                         Comparator<CSVRecord> cmp, boolean isDinstinct, CSVFormat csvFormat)
            throws IOException {
        // TODO: ADD YOUR CODE HERE

        PriorityQueue<CsvFilelineStack> pq = new PriorityQueue<>(
                new Comparator<CsvFilelineStack>() {
                    @Override
                    public int compare(CsvFilelineStack i, CsvFilelineStack j) {
                        return cmp.compare(i.peek(),j.peek()); // 比较每个文件栈顶,也就是最后一个元素的大小
                    }
                }
        ); //使用给定的比较器进行排序,小顶堆
        for (CsvFilelineStack csvFilelineStack : csvList) { // 将每个run入堆
            if (!csvFilelineStack.empty()){
                pq.add(csvFilelineStack);
            }
        }
        long rowCnt = 0;
        CSVPrinter printer = new CSVPrinter(bufferedWriter, csvFormat);
        CSVRecord lastLine = null;
        try {
            while (!pq.isEmpty()) {
                CsvFilelineStack csvFileLines = pq.poll(); // 获取并删除队首元素, 得到所有run文件中,record最小的那个
                CSVRecord curLine = csvFileLines.pop(); // 获取当前文件的第一个record
                if (curLine == null) {
                    throw new IllegalStateException("INVALID AREA");
                }
                if (isDinstinct && (lastLine != null && cmp.compare(curLine, lastLine) == 0)) {
                    LOG.warn("skip one line because key is not distinct {}", curLine.toString());
                } else {
                    printer.printRecord(curLine); // 将这个record打印到文件中
                    lastLine = curLine;
                    rowCnt++;
                }
                if (csvFileLines.empty()) {
                    csvFileLines.close();
                } else {
                    pq.offer(csvFileLines); /*boolean offer(Object e);
                    // 将指定元素加入此队列的尾部。当使用有容量限制的队列时，此方法通常比add(Object e)方法更好。*/
                }
            }
        } finally {
            printer.close();
            bufferedWriter.close();
            for (CsvFilelineStack bfb : pq) {
                bfb.close();
            }
        }
        return rowCnt;
    }
}
