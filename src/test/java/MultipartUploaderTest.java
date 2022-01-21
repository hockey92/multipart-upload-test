import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.internal.Constants;
import com.amazonaws.services.s3.model.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

public class MultipartUploaderTest {
    private static final String JSON = "[{\"type\":\"configuration/entityTypes/person\",\"attributes\":{\"birthDetails\":[{\"value\":{\"birthDate\":[{\"value\":\"1999-06-27\"}]}}],\"address\":[{\"value\":{\"derivedISOCountryCode\":[{\"value\":\"USA\"}],\"localityName\":[{\"value\":\"SAN JOSE\"}],\"derivedPostalCode\":[{\"value\":\"951171239\"}],\"addressLastUpdatedDate\":[{\"value\":\"2009-10-20T09:45:43.285-0800\"}],\"addressType\":[{\"value\":\"Legal\"}],\"isoCountryCode\":[{\"value\":\"USA\"}],\"postalCode\":[{\"value\":\"951171239\"}],\"regionName\":[{\"value\":\"CA\"}],\"addressLine1\":[{\"value\":\"550 KIELY BLVD #79\"}],\"addressEffectiveDate\":[{\"value\":\"1987-06-29T23:46:55.896-0800\"}],\"addressSourceCode\":[{\"value\":\"Employer\"}],\"addressStatusCode\":[{\"value\":\"KJTFUYSOOADOPBKR\"}]}}],\"givenName\":[{\"value\":{\"givenName1\":[{\"value\":\"SKFRWQXCAZJBZEM\"}],\"surName1\":[{\"value\":\"ANYTTACWXNFZA\"}]}}],\"businessContext\":[{\"value\":{\"internationalIndicator\":[{\"value\":\"LALLGKZNRGJEIWJK\"}],\"businessRelationship\":[{\"value\":\"WorkplaceServices\"}],\"businessPersona\":[{\"value\":\"PrincipalAccountHolder\"}]}}],\"dataCustodian\":[{\"value\":{\"dataCustodianId\":[{\"value\":\"3356\"}],\"dataCustodianContext\":[{\"value\":\"Self\"}],\"dataCustodianIdType\":[{\"value\":\"WID\"}]}}],\"clientProvidedId\":[{\"value\":{\"clientProvidedPersonId\":[{\"value\":\"490-35-7555\"}],\"clientProvidedIdType\":[{\"value\":\"EEID\"}]}}],\"fidelitySourceId\":[{\"value\":{\"personSourceIdType\":[{\"value\":\"SPSPARTID\"}],\"personSourceId\":[{\"value\":\"0\"}],\"sourceSystemCode\":[{\"value\":\"WSMDM\"}]}}],\"dataSupplier\":[{\"value\":{\"dataSupplierIdType\":[{\"value\":\"ORGID\"}],\"dataSupplierId\":[{\"value\":\"7938\"}],\"dataSupplierContext\":[{\"value\":\"Client\"}]}}],\"governmentId\":[{\"value\":{\"issuingAuthority\":[{\"value\":\"LZPZHTMTPBJI\"}],\"governmentIdType\":[{\"value\":\"SSN\"}],\"id\":[{\"value\":\"853-73-2812\"}]}}]},\"crosswalks\":[{\"type\":\"configuration/sources/ICSFBS\",\"value\":\"1#0\"}]}]";
    private static final int PART_SIZE = 5 * Constants.MB;
    private static final String AWS_KEY = ;
    private static final String AWS_SECRET_KEY = ;
    private static final String AWS_REGION = "us-east-1";
    private static final String S3_BUCKET = "alexey.matveev";
    private static final String S3_FILENAME = "MultipartUploaderTest.txt";

    private static final Charset CHARSET = StandardCharsets.UTF_8;

    private AmazonS3 amazonS3;

    @Before
    public void prepareTest() {
        amazonS3 = amazonS3(AWS_KEY, AWS_SECRET_KEY, AWS_REGION);
        deleteFile();
    }

    @After
    public void afterTest() {
        deleteFile();
    }

    @Test
    public void test() throws IOException {
        test(
                //parts to upload
                Arrays.asList(1, 2, 3, 4),
                //parts to combine
                Arrays.asList(1, 2)
        );
    }

    @Test
    public void test1() throws IOException {
        test(
                //parts to upload
                Arrays.asList(100, 243, 4, 35),
                //parts to combine
                Arrays.asList(243, 4)
        );
    }

    private void test(List<Integer> partsToLoad, List<Integer> partsToCombine) throws IOException {
        String uploadId = initiateMultipartUpload();

        Map<Integer, Integer> partNumberToNumberOfRecords = new TreeMap<>();
        Map<Integer, PartETag> partNumberToTag = new TreeMap<>();
        for (int partNumber : partsToLoad) {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            OutputStreamWriter streamWriter = new OutputStreamWriter(outputStream, CHARSET.newEncoder());

            for (int recordNumber = 0; ; recordNumber++) {

                streamWriter.write(buildRecord(partNumber, recordNumber));
                streamWriter.write('\n');

                if (outputStream.size() > PART_SIZE) {
                    streamWriter.flush();

                    PartETag tag = uploadPart(uploadId, partNumber, outputStream.toByteArray());
                    partNumberToTag.put(partNumber, tag);
                    System.out.println(partNumber + " part was uploaded");

                    outputStream.reset();

                    partNumberToNumberOfRecords.put(partNumber, recordNumber + 1);

                    break;
                }
            }
        }

        System.out.println("Listing parts...");
        PartListing partListing = listParts(uploadId);
        System.out.println("Loaded parts:");
        System.out.println("==========================================================");
        for (PartSummary partSummary : partListing.getParts()) {
            System.out.println("partNumber = " + partSummary.getPartNumber());
            System.out.println("size = " + partSummary.getSize());
            System.out.println("lastModified = " + partSummary.getLastModified());
            System.out.println("==========================================================");
        }

        System.out.println("Creating final file...");
        completeMultipartUploadRequest(
                uploadId,
                partsToCombine.stream().map(partNumberToTag::get).collect(Collectors.toList())
        );

        System.out.println("Checking the final file...");
        checkFinalFile(partNumberToNumberOfRecords, partsToCombine);
    }

    private void checkFinalFile(Map<Integer, Integer> partNumberToNumberOfRecords,
                                List<Integer> partNumbersToCombine) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                amazonS3.getObject(S3_BUCKET, S3_FILENAME).getObjectContent()
        ))) {
            for (Integer partNumber : partNumbersToCombine.stream().sorted().collect(Collectors.toList())) {
                int numberOfRecords = partNumberToNumberOfRecords.get(partNumber);

                System.out.println("Checking " + partNumber + " part...");
                for (int recordNumber = 0; recordNumber < numberOfRecords; recordNumber++) {
                    String line = reader.readLine();
                    Assert.assertEquals(buildRecord(partNumber, recordNumber), line);
                }
            }
            Assert.assertNull(reader.readLine());
        }
        System.out.println("SUCCESS");
    }

    private PartListing listParts(String uploadId) {
        ListPartsRequest listPartsRequest = new ListPartsRequest(S3_BUCKET, S3_FILENAME, uploadId);
        return amazonS3.listParts(listPartsRequest);
    }

    private String initiateMultipartUpload() {
        InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(S3_BUCKET, S3_FILENAME);
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentType("text/json");
        metadata.setContentEncoding(CHARSET.name());
        initRequest.setObjectMetadata(metadata);
        return amazonS3.initiateMultipartUpload(initRequest).getUploadId();
    }

    private PartETag uploadPart(String uploadId, int partNumber, byte[] byteArray) {
        UploadPartRequest uploadRequest = new UploadPartRequest()
                .withBucketName(S3_BUCKET)
                .withKey(S3_FILENAME)
                .withUploadId(uploadId)
                .withPartNumber(partNumber)
                .withPartSize(byteArray.length)
                .withLastPart(false)
                .withInputStream(new ByteArrayInputStream(byteArray));
        System.out.println("Uploading " + partNumber + " part...");
        return amazonS3.uploadPart(uploadRequest).getPartETag();
    }

    private void completeMultipartUploadRequest(String uploadId, List<PartETag> partETags) {
        amazonS3.completeMultipartUpload(new CompleteMultipartUploadRequest(
                S3_BUCKET,
                S3_FILENAME,
                uploadId,
                partETags
        ));
    }

    private void deleteFile() {
        amazonS3.deleteObject(S3_BUCKET, S3_FILENAME);
        System.out.println("File was removed");
    }

    private static AmazonS3 amazonS3(String awsKey, String awsSecretKey, String awsRegion) {
        AWSCredentials credentials = new BasicAWSCredentials(
                awsKey,
                awsSecretKey
        );
        return AmazonS3ClientBuilder.standard()
                .withRegion(awsRegion)
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .build();
    }

    private static String buildRecord(int partNumber, int recordNumber) {
        return partNumber + "#" + recordNumber + "|" + JSON;
    }
}
