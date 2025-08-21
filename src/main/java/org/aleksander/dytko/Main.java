package org.aleksander.dytko;

import io.camunda.client.CamundaClient;
import io.camunda.client.CredentialsProvider;
import io.camunda.client.api.response.ActivatedJob;
import io.camunda.client.api.response.DeploymentEvent;
import io.camunda.client.api.response.ProcessInstanceEvent;
import io.camunda.client.api.response.Topology;
import io.camunda.client.api.worker.JobClient;
import io.camunda.client.api.worker.JobHandler;
import io.camunda.client.api.worker.JobWorker;
import io.camunda.client.impl.NoopCredentialsProvider;
import io.camunda.client.impl.basicauth.BasicAuthCredentialsProvider;
import io.camunda.client.impl.basicauth.BasicAuthCredentialsProviderBuilder;

import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.Scanner;

/**
 * Main class to connect to Camunda 8 Gateway using gRPC and REST.
 * Replace the grpcAddress and restAddress with your actual addresses.
 */
public class Main {

    private static final String CAMUNDA_GRPC_ADDRESS = "http://localhost:26500";
    private static final String CAMUNDA_REST_ADDRESS = "http://localhost:8080";
    private static final String CAMUNDA_CLIENT_USERNAME = "aleks"; // or your username
    private static final String CAMUNDA_CLIENT_PASSWORD = "demo"; // or your password

    public static void main(String[] args) {

        // For local development (no authentication)
        //CredentialsProvider credentialsProvider = new NoopCredentialsProvider();


        CredentialsProvider credentialsProvider = new BasicAuthCredentialsProviderBuilder()
                .username(CAMUNDA_CLIENT_USERNAME)
                .password(CAMUNDA_CLIENT_PASSWORD)
                .build();



        try (

                /*
                CamundaClient client = CamundaClient.newCloudClientBuilder()
                .withClusterId("7f6f3276-3877-4aa6-a0c0-96594f6ac7c7")
                .withClientId("c7YdKK4SLKtxXvX_jF8J_~eEwJas3.kX")
                .withClientSecret("aXZNP0NLdRyaeJScAxAvWkv4.7EcHKMTdbAlU6urxE3-JDil_D4gX1zjjtQ7jBtA")
                .withRegion("fra-1")
                .build())

                 */




                // For local development (no authentication)
                CamundaClient client = CamundaClient.newClientBuilder()
                .grpcAddress(URI.create(CAMUNDA_GRPC_ADDRESS)).usePlaintext()
                .restAddress(URI.create(CAMUNDA_REST_ADDRESS))
                .preferRestOverGrpc(true)
                .credentialsProvider(credentialsProvider)
                .build())





                //CamundaClient client = CamundaClient.newClientBuilder().usePlaintext().build())




        {
            // Test the connection
            Topology topology= client.newTopologyRequest().send().join();
            System.out.println(topology);

            // Deploy a process definition
            System.out.println("Deploying process definition...");
            final DeploymentEvent deploymentEvent = client.newDeployResourceCommand()
                    .addResourceFromClasspath("exampleProcess.bpmn")
                    .send()
                    .join();

            System.out.println("Deployment successful: " + deploymentEvent.getKey());

            // Create a process instance
            System.out.println("Creating process instance...");
            final ProcessInstanceEvent processInstanceEvent = client.newCreateInstanceCommand()
                    .bpmnProcessId("exampleProcess")
                    .latestVersion()
                    .variables(Map.of("orderId", "12345"))
                    .send()
                    .join();

            System.out.println("Process instance created: " + processInstanceEvent.getProcessInstanceKey());

            // Register a job worker to handle jobs of type "send-email"
            System.out.println("Registering job worker...");

            // The job type must match the one defined in the BPMN process
            final String jobType = "send-email";

            try (final JobWorker workerRegistration = client.newWorker()
                    .jobType(jobType)
                    .handler(new ExampleJobHandler())
                    .timeout(Duration.ofSeconds(10))
                    .name("Aleksander's Job Worker")
                    .open()) {

                System.out.println("Job worker opened and receiving jobs.");

                // run until System.in receives exit command
                waitUntilSystemInput("exit");
            }


            }
        }

    private static void waitUntilSystemInput(final String exitCode) {
        try (final Scanner scanner = new Scanner(System.in)) {
            while (scanner.hasNextLine()) {
                final String nextLine = scanner.nextLine();
                if (nextLine.contains(exitCode)) {
                    return;
                }
            }
        }
    }

    private static class ExampleJobHandler implements JobHandler {
        @Override
        public void handle(final JobClient client, final ActivatedJob job) {
            // here: business logic that is executed with every job
            System.out.println(job);
            client.newCompleteCommand(job.getKey()).send().join();
        }
    }
}