package com.externalsort;

import com.externalsort.helper.FileEncrypterDecrypter;
import com.externalsort.helper.IStreamWrapper;
import com.externalsort.helper.RandomString;
import com.externalsort.helper.TmpFileBuilder;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.junit.Ignore;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.junit.Assert.*;


public class ExternalSortTest {

    Comparator<CSVRecord> DEFAULT_CMP = (a, b) -> a.get(0).compareTo(b.get(0));
    IStreamWrapper DEFAULT_WRAPPER = new IStreamWrapper() {
    };

    @Test
    public void testEmptyFiles() throws Exception {
        File f1 = File.createTempFile("tmp", "unit");
        File f2 = File.createTempFile("tmp", "unit");
        f1.deleteOnExit();
        f2.deleteOnExit();
        ExternalSort.sortCsv(f1, f2, DEFAULT_CMP);
        if (f2.length() != 0) throw new RuntimeException("empty files should end up emtpy");
    }


    @Test
    @Ignore
    public void testRandomContentFiles() throws Exception {
        File out = File.createTempFile("test_results", ".tmp");
        out.deleteOnExit();

        List<List<String>> fileContent = new ArrayList<>();
        List<String> expect = new ArrayList<>();
        List<File> toBeMerged = new ArrayList<>();
        for (int j = 0; j < 5; j++) {
            fileContent.add(new ArrayList<>());
            for (int k = 0; k < 10; k++) {
                String generatedString = RandomString.random(1 + (int) (Math.random() * 10));
                fileContent.get(fileContent.size() - 1).add(generatedString);
                expect.add(generatedString);
            }
            Collections.sort(fileContent.get(fileContent.size() - 1));
            toBeMerged.add(TmpFileBuilder.tmpFileBuilder(fileContent.get(fileContent.size() - 1)));
        }

        SortedFilesMerger.mergeSortedCsvFiles(toBeMerged, out, CSVFormat.DEFAULT, DEFAULT_CMP,
                false, false, DEFAULT_WRAPPER);

        Collections.sort(expect);
        int idx = 0;
        try (BufferedReader reader = new BufferedReader(new FileReader(out))) {
            String line = null;
            while ((line = reader.readLine()) != null) {
                assertEquals(expect.get(idx++), line);
            }
        }
        assertTrue(idx == expect.size());
    }

    @Test
    @Ignore
    public void testWithCustomCmp() throws Exception {
        File out = File.createTempFile("test_results", ".tmp");
        out.deleteOnExit();

        Comparator<String> customCmp = (a, b) -> {
            String aSuffix = a.substring(a.indexOf(",")), bSuffix = b.substring(b.indexOf(","));
            return aSuffix.compareTo(bSuffix);
        };

        List<List<String>> fileContent = new ArrayList<>();
        List<String> expect = new ArrayList<>();
        List<File> toBeMerged = new ArrayList<>();
        for (int j = 0; j < 5; j++) {
            fileContent.add(new ArrayList<>());
            for (int k = 0; k < 10; k++) {
                String secondCol = RandomString.random(1 + (int) (Math.random() * 10));
                String generatedString = ((int) (Math.random() * 1000)) + "," + secondCol;
                fileContent.get(fileContent.size() - 1).add(generatedString);
                expect.add(generatedString);
            }
            Collections.sort(fileContent.get(fileContent.size() - 1), customCmp);
            toBeMerged.add(TmpFileBuilder.tmpFileBuilder(fileContent.get(fileContent.size() - 1)));
        }

        SortedFilesMerger.mergeSortedCsvFiles(toBeMerged, out, CSVFormat.DEFAULT, (a, b) -> a.get(1).compareTo(b.get(1)),
                false, false, DEFAULT_WRAPPER);

        Collections.sort(expect, customCmp);
        int idx = 0;
        try (BufferedReader reader = new BufferedReader(new FileReader(out))) {
            String line = null;
            while ((line = reader.readLine()) != null) {
                assertEquals(expect.get(idx++), line);
            }
        }
        assertTrue(idx == expect.size());
    }

    @Test
    @Ignore
    public void testIntermediateFileDeleted() throws IOException {
        String unsortedContent =
                "Val1,Data2,Data3,Data4\r\n" +
                        "Val2,Data2,Data4,Data5\r\n" +
                        "Val1,Data2,Data3,Data5\r\n" +
                        "Val2,Data2,Data6,Data7\r\n";
        File tmpDirectory = Files.createTempDirectory("sort").toFile();
        tmpDirectory.deleteOnExit();

        File inputCsv = TmpFileBuilder.tmpFileBuilder(unsortedContent);
        List<File> tmpSortedFiles = BatchSortedFileProducer.sortCsvInBatch(
                inputCsv,
                DEFAULT_CMP,
                tmpDirectory,
                Charset.defaultCharset(),
                false,
                CSVFormat.DEFAULT,
                0);
        File tmpOutputFile = File.createTempFile("merged", "", tmpDirectory);
        tmpOutputFile.deleteOnExit();
        SortedFilesMerger.mergeSortedCsvFiles(
                tmpSortedFiles,
                tmpOutputFile,
                CSVFormat.DEFAULT,
                DEFAULT_CMP,
                false,              // no distinct
                false);             // don't use gzip

        for (File tmpSortedFile : tmpSortedFiles) {
            assertFalse(tmpSortedFile.exists());
        }
    }


    @Test
    @Ignore
    public void testSortDistinct() throws IOException {
        String resourcesPath = getClass().getResource("/test_files/").getPath();
        File input = new File(resourcesPath + "external_mission.csv");
        File output = new File(resourcesPath + "external_mission_distinct.csv");
        ExternalSort.sortCsv(input, output, (a, b) -> a.get(0).compareTo(b.get(0)),
                true, false, 0);
        checkFileIsSorted(10, output);
    }

    @Test
    @Ignore
    public void testSortNonDistinct() throws IOException {
        String resourcesPath = getClass().getResource("/test_files/").getPath();
        File input = new File(resourcesPath + "external_mission.csv");
        File output = new File(resourcesPath + "external_mission_nondistinct.csv");
        ExternalSort.sortCsv(input, output, (a, b) -> a.get(0).compareTo(b.get(0)),
                false, false, 0);
        checkFileIsSorted(500000, output);
    }

    private void checkFileIsSorted(long expectLines, File output) throws IOException {
        try {
            long curLineCnt = 0;
            String prevLine = null;
            try (BufferedReader br = new BufferedReader(new FileReader(output))) {
                String curLine = null;
                while ((curLine = br.readLine()) != null) {
                    if (prevLine != null) {
                        assertTrue(curLine.compareTo(prevLine) >= 0);
                    }
                    prevLine = curLine;
                    curLineCnt++;
                }
            }
            assertEquals(expectLines, curLineCnt);
        } finally {
            if (output.exists()) {
                output.delete();
            }
        }

    }


    @Test
    // @Ignore
    public void testEncryptedExternalSortFile() throws Exception {

        FileEncrypterDecrypter fileEncrypterDecrypter
                = new FileEncrypterDecrypter();

        String resourcesPath = getClass().getResource("/test_files/").getPath();
        File input = new File(resourcesPath + "external_mission.csv.aes");
        File output = new File(resourcesPath + "encrypt_external_mission.csv.aes");
        ExternalSort.sortCsv(input, output, (a, b) -> a.get(0).compareTo(b.get(0)),
                false, false, Charset.defaultCharset(), CSVFormat.DEFAULT, 0,
                fileEncrypterDecrypter);

        File decryptOutput = new File(resourcesPath + "sorted_external_mission.csv");
        fileEncrypterDecrypter.decrypt(output.getPath(),
                decryptOutput.getPath());

        checkFileIsSorted(500000, decryptOutput);
    }


    @Test
    @Ignore("This test takes too long to execute")
    public void sortVeryLargeFile() throws IOException {
        final int blockLines = 20000, blockNums = 2048;
        final File veryLargeFile = TmpFileBuilder.getSuperHugeTestFile(blockNums, blockLines);
        veryLargeFile.deleteOnExit();
        final File outputFile = File.createTempFile("Merged-File", ".tmp");
        outputFile.deleteOnExit();
        final long sortedLines = ExternalSort.sortCsv(veryLargeFile, outputFile, DEFAULT_CMP);
        final long expectedLines = (long) blockNums * blockLines;
        assertEquals(expectedLines, sortedLines);
        checkFileIsSorted(expectedLines, outputFile);
    }


    @Test
    @Ignore(" this is for debug ")
    public void decryptTool() throws Exception {
        FileEncrypterDecrypter fileEncrypterDecrypter
                = new FileEncrypterDecrypter();

        String outputPath = "src/test/resources/output/";

        fileEncrypterDecrypter.decrypt("", outputPath + "decrypt_a.csv");
    }

}