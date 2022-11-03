package org.example;


import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.waiters.S3Waiter;
import software.amazon.awssdk.services.sfn.SfnClient;
import software.amazon.awssdk.services.sfn.model.*;

import java.io.File;
import java.util.List;

public class Main {

    private static final String bucket = "upgradeprospectetl-bucket-ingestion-staging";
    private static final String runId = "0bb5c200-6a4e-4f94-b8ad-1df4524eada1";
    private static final String campaignId = "17260442-9dd8-4a40-a6ab-6f40cfc0cb20";
    private static final String tableName = "Manav";
    private static final String fileName = "Upgrde1.csv";
    private static final String path = runId + "/" + campaignId + "/" + tableName + "/" + fileName;

    private static final String stateMchine = "UpgradeProspectEtl-SM-Staging";
    private static final String arn = "arn:aws:states:us-east-1:909837171498:execution:UpgradeProspectEtl-SM-Staging:54ab6b75-2245-9ccd-2f12-bf88da9964d9_4d696468-b1e9-b740-7f76-955516627822";

    public static void main(String[] args) {
        AWSS3BucketName();
        // fileUploadToAWSS3();
        AWSResponseSfn();
    }

    public static void AWSS3BucketName() {
        S3Client s3 = S3Client.create();
        ListBucketsResponse response = s3.listBuckets();
        for (Bucket bucket : response.buckets()) {
            System.out.println(bucket.name());
        }
    }

    public static void fileUploadToAWSS3() {
        S3Client s3 = S3Client.create();

        PutObjectResponse putObjectResponse = s3.putObject(PutObjectRequest.builder()
                .bucket(bucket).key(path)
                .build(), RequestBody.fromFile(new File("/Users/manav/Downloads/EtlAuto.csv"))
        );

        System.out.println(putObjectResponse);

        S3Waiter waiter = s3.waiter();
        HeadObjectRequest requestWait = HeadObjectRequest.builder().bucket(bucket).key(path).build();

        WaiterResponse<HeadObjectResponse> waiterResponse = waiter.waitUntilObjectExists(requestWait);

        waiterResponse.matched().response().ifPresent(System.out::println);

        System.out.println("File " + fileName + " was uploaded.");

    }

    public static void AWSResponseSfn() {
        SfnClient sfn = SfnClient.create();
        ListStateMachinesResponse response = sfn.listStateMachines();
        List<StateMachineListItem> machines = response.stateMachines();
        for (StateMachineListItem machine : machines) {
            System.out.println("The name of the state machine is: " + machine.name());
            System.out.println("The ARN value is : " + machine.stateMachineArn());
        }
        GetExecutionHistoryRequest historyRequest = GetExecutionHistoryRequest.builder()
                .executionArn(arn)
                .maxResults(10)
                .build();
        GetExecutionHistoryResponse historyResponse = sfn.getExecutionHistory(historyRequest);
        List<HistoryEvent> events = historyResponse.events();
        for (HistoryEvent event : events) {
            System.out.println("The event type is " + event.executionSucceededEventDetails());
        }


    }
}