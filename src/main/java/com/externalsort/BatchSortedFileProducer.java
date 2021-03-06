package com.externalsort;

import com.externalsort.helper.IStreamWrapper;
import com.externalsort.helper.SizeCalculator;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.*;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class BatchSortedFileProducer {

    private static final Logger LOG = LoggerFactory.getLogger(BatchSortedFileProducer.class);


    public static List<File> sortCsvInBatch(File csvFile, Comparator<CSVRecord> cmp, File tmpDirectory, Charset cs,
                                            boolean isDistinct, CSVFormat csvFormat, int excludeHeaderLines)
            throws IOException{
        return sortCsvInBatch(csvFile, cmp, tmpDirectory, cs, isDistinct, csvFormat, excludeHeaderLines,
                new IStreamWrapper(){});

    }

    /**
     * Given a huge CSV file, This will simply load the file by blocks of lines, then sort them
     * in-memory, and write the result to temporary files that have to be merged later.
     * @param csvFile input CSV file
     * @param cmp comparator passed by user
     * @param tmpDirectory the directory path where to save the tmp file
     * @param cs charset to parse the file
     * @param isDistinct is the comparator key is distinct which means no two record are compare == 0
     *                  if isDistinct == true, but the data is not distinct, it will ignore the duplicate key randomly
     * @param csvFormat the CSV Format you want, if no idea, use CSVFormat.DEFAULT
     * @param excludeHeaderLines how many line in csv header, this should be precluded before sorting starts
     * @param wrapper this used for Stream Wrapper, eg. some one want to use AES Stream to protect file Stream
     * @return result to temporary files that have to be merged later
     * @throws IOException
     */
    public static List<File> sortCsvInBatch(File csvFile, Comparator<CSVRecord> cmp, File tmpDirectory, Charset cs,
                                            boolean isDistinct, CSVFormat csvFormat, int excludeHeaderLines,
                                            IStreamWrapper wrapper) throws IOException {

        // TODO: ADD YOUR CODE HERE
        List<File> files = new ArrayList<>();
        long blockSize = estimateBestSizeOfBlocks(estimateAvailableMemory()); // ?????????????????????????????????run??????
        /*
        * AtomicLong?????????????????????????????????????????????
        * ???32?????????????????????64??????long ??? double ??????????????????JVM?????????????????????32????????????????????????????????????????????????
        * ?????????AtomicLong??????long???????????????????????????
        * */
        AtomicLong currentBlock = new AtomicLong(0); // ????????????????????????????????????,??????????????????
        List<CSVRecord> tmpList = new ArrayList<CSVRecord>();  //????????????run??????record list
        AtomicInteger cnt = new AtomicInteger(0);

        // ???????????????CSV??????,???????????????????????????
        try (CSVParser parser = new CSVParser(new BufferedReader(
                new InputStreamReader(wrapper.wrap(new FileInputStream(csvFile)), cs)),
                CSVFormat.DEFAULT)) {
            parser.spliterator().forEachRemaining(e -> {
                cnt.getAndIncrement(); // cnt ?????????1
                if (currentBlock.get() < blockSize) { // ???????????????????????????blockSize,???????????????
                    if (e.getRecordNumber() <= excludeHeaderLines) { // ????????????headerlines???
                        // csv ?????????headline
                        LOG.info("skip one header line because key is not distinct {}", e.toString());
                    } else { // ??????headerlines??????????????????
                        tmpList.add(e);
                        currentBlock.addAndGet(SizeCalculator.estimatedSizeOf(e));
                    }
                } else { // ????????????????????? ??????blockSize
                    try {
                        tmpList.add(e);
                        files.add(sortSingleTmpFile(tmpList,cmp,tmpDirectory,isDistinct,csvFormat,wrapper));
                    } catch (Exception e1) {
                        LOG.warn("Error during the sort in batch",e1);
                    }
                    tmpList.clear();
                    currentBlock.getAndSet(0);
                }
            });
        }
        if (!tmpList.isEmpty()) {
            files.add(sortSingleTmpFile(tmpList, cmp, tmpDirectory, isDistinct, csvFormat, wrapper)); // ???????????????record????????????
        }
        return files;
    }

    /**
     * Sort a list and save it to a temporary file
     * @param tmpList data to be sorted
     * @param cmp comparator passed by user
     * @param tmpFolderPath the directory path where to save the tmp file
     * @param isDistinct is the comparator key is distinct which means no two record are compare == 0
     *                   if isDistinct == true, but the data is not distinct, it will ignore the duplicate key randomly
     * @param csvFormat the CSV Format you want, if no idea, pass CSVFormat.DEFAULT
     * @param wrapper this used for Stream Wrapper, eg. some one want to use AES Stream to protect file Stream
     * @return the tmp file which content is data in tmpList sorted
     * @throws IOException
     */
    private static File sortSingleTmpFile(List<CSVRecord> tmpList, Comparator<CSVRecord> cmp,
                                          File tmpFolderPath, boolean isDistinct, CSVFormat csvFormat,
                                          IStreamWrapper wrapper) throws IOException {
        // TODO: ADD YOUR CODE HERE
        Collections.sort(tmpList,cmp);
        File tmpFile = File.createTempFile("sortInBatch",".run",tmpFolderPath);
        CSVRecord lastLine = null;
        try (CSVPrinter printer = new CSVPrinter(new BufferedWriter(
                new OutputStreamWriter(wrapper.wrap(new FileOutputStream(tmpFile)))),csvFormat)) {
            for (CSVRecord csvrecord : tmpList) {
                if (csvrecord == null) {
                    throw new IllegalStateException("INVALID AREA");
                }
                if (isDistinct && (lastLine != null && cmp.compare(csvrecord,lastLine) ==0)) { // isDistinct ????????????????????????
                    LOG.warn("skip one line because key is not distinct {} ",csvrecord.toString());
                } else {
                    printer.printRecord(csvrecord);
                    lastLine = csvrecord;
                }
            }
        }
        return tmpFile;
    }

    /**
     * This method calls the garbage collector and then returns the free
     * memory. This avoids problems with applications where the GC hasn't
     * reclaimed memory and reports no available memory.
     *
     * @return available memory
     */
    private static long estimateAvailableMemory() {
        System.gc();
        return Runtime.getRuntime().freeMemory();
    }

    /**
     * we divide the file into small blocks. If the blocks are too small, we
     * shall create too many temporary files. If they are too big, we shall
     * be using too much memory. So avoid to have OutofMemoryError, we choose maxMemory/6
     * as a block size
     *
     * @param maxMemory Maximum memory to use (in bytes)
     * @return the estimate
     */
    private static long estimateBestSizeOfBlocks(final long maxMemory) {
        return maxMemory / 2;
    }

}
