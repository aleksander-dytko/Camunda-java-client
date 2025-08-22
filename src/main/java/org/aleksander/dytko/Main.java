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
import io.camunda.client.impl.oauth.OAuthCredentialsProviderBuilder;

import java.time.Duration;
import java.util.Map;

/**
 * Main class to connect to Camunda 8 Gateway using gRPC and REST.
 * Replace the grpcAddress and restAddress with your actual addresses.
 */
public class Main {

    private static final String CAMUNDA_GRPC_ADDRESS = "grpcs://7f6f3276-3877-4aa6-a0c0-96594f6ac7c7.fra-1.zeebe.camunda.io:443";
    private static final String CAMUNDA_REST_ADDRESS = "https://fra-1.zeebe.camunda.io/7f6f3276-3877-4aa6-a0c0-96594f6ac7c7";
    private static final String CAMUNDA_CLIENT_USERNAME = "aleks"; // or your username
    private static final String CAMUNDA_CLIENT_PASSWORD = "demo"; // or your password
    private static final String oauthUrl = "https://login.cloud.camunda.io/oauth/token";
    private static final String audience = "zeebe.camunda.io";
    private static final String clientId = "7f6f3276-3877-4aa6-a0c0-96594f6ac7c7";
    private static final String clientSecret = "tN5smyD.2IgBDe2kWi.lsVs6JdQSWMzcb6fV2FWv7qXpTryCW75Fe~H0Dv15-BWj";

    public static void main(String[] args) {

        // For local development (no authentication)
        //CredentialsProvider credentialsProvider = new NoopCredentialsProvider();


        /*
        CredentialsProvider credentialsProvider = new BasicAuthCredentialsProviderBuilder()
                .username(CAMUNDA_CLIENT_USERNAME)
                .password(CAMUNDA_CLIENT_PASSWORD)
                .build();

         */


        /*
        CredentialsProvider credentialsProvider = new OAuthCredentialsProviderBuilder()
                .authorizationServerUrl(oauthUrl)
                .audience(audience)
                .clientId(clientId)
                .clientSecret(clientSecret)
                .build();

         */





        try (

                /*
                CamundaClient client = CamundaClient.newCloudClientBuilder()
                .withClusterId("7f6f3276-3877-4aa6-a0c0-96594f6ac7c7")
                .withClientId("c7YdKK4SLKtxXvX_jF8J_~eEwJas3.kX")
                .withClientSecret("aXZNP0NLdRyaeJScAxAvWkv4.7EcHKMTdbAlU6urxE3-JDil_D4gX1zjjtQ7jBtA")
                .withRegion("fra-1")
                .build())

                 */


                /*
                // For local development (no authentication)
                CamundaClient client = CamundaClient.newClientBuilder()
                .grpcAddress(URI.create(CAMUNDA_GRPC_ADDRESS)).usePlaintext()
                .restAddress(URI.create(CAMUNDA_REST_ADDRESS))
                .credentialsProvider(credentialsProvider)
                .build())

                 */

                /*

                CamundaClient client = CamundaClient.newClientBuilder()
                .grpcAddress(URI.create(CAMUNDA_GRPC_ADDRESS))
                .restAddress(URI.create(CAMUNDA_REST_ADDRESS))
                .credentialsProvider(credentialsProvider)
                .build())

                 */



                CamundaClient client = CamundaClient.newClientBuilder().build())




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
                    .variables(Map.of("orderId", "12345", "amount", 100.0))
                    .send()
                    .join();

            System.out.println("Process instance created: " + processInstanceEvent.getProcessInstanceKey());

            // Register a job worker to handle jobs of type "send-email"
            System.out.println("Registering job worker...");

            // The job type must match the one defined in the BPMN process
            final String jobType = "send-email";

            try (final JobWorker workerRegistration = client.newWorker()
                    .jobType(jobType)
                    .handler(new EmailJobHandler())
                    .open()) {

                System.out.println("Job worker opened and receiving jobs of type: " + jobType);

                // Keep the worker running
                Thread.sleep(Duration.ofMinutes(10));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        }

    private static class EmailJobHandler implements JobHandler {
        @Override
        public void handle(final JobClient client, final ActivatedJob job) {
            // Extract variables from the job
            final Map<String, Object> variables = job.getVariablesAsMap();

            // Perform your business logic here
            System.out.println("Processing job: " + job.getKey() + " for a process instance: " + job.getProcessInstanceKey());

            // Complete the job (or use client.newFailCommand() if something goes wrong)
            client.newCompleteCommand(job.getKey())
                    .variables(Map.of("emailSent", true))
                    .send()
                    .join();
        }
    }
}