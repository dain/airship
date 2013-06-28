package io.airlift.airship.agent;

import com.google.common.collect.ImmutableMap;
import com.google.common.net.MediaType;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.airlift.airship.shared.AirshipHeaders;
import io.airlift.airship.shared.HttpUriBuilder;
import io.airlift.airship.shared.Installation;
import io.airlift.airship.shared.InstallationHelper;
import io.airlift.airship.shared.job.SlotJob;
import io.airlift.airship.shared.job.SlotJobStatus;
import io.airlift.airship.shared.job.SlotJobStatus.SlotJobState;
import io.airlift.configuration.ConfigurationFactory;
import io.airlift.configuration.ConfigurationModule;
import io.airlift.discovery.client.testing.TestingDiscoveryModule;
import io.airlift.event.client.EventModule;
import io.airlift.http.client.ApacheHttpClient;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.http.server.testing.TestingHttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.json.JsonModule;
import io.airlift.node.testing.TestingNodeModule;
import io.airlift.units.Duration;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

import java.io.File;
import java.net.URI;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static io.airlift.airship.shared.FileUtils.createTempDir;
import static io.airlift.airship.shared.FileUtils.deleteRecursively;
import static io.airlift.airship.shared.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static io.airlift.json.JsonCodec.jsonCodec;
import static org.testng.Assert.assertTrue;

public class TestServerLifecycleJobs
        extends AbstractTestLifecycleJobs
{
    private HttpClient client;
    private TestingHttpServer server;

    private Agent agent;

    private InstallationHelper installationHelper;
    private Installation appleInstallation;
    private Installation bananaInstallation;
    private File tempDir;

    @BeforeClass
    public void startServer()
            throws Exception
    {
        tempDir = createTempDir("agent");
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("agent.id", UUID.randomUUID().toString())
                .put("agent.coordinator-uri", "http://localhost:9999/")
                .put("agent.slots-dir", tempDir.getAbsolutePath())
                .put("discovery.uri", "fake://server")
                .build();

        Injector injector = Guice.createInjector(
                new TestingDiscoveryModule(),
                new TestingNodeModule(),
                new JsonModule(),
                new TestingHttpServerModule(),
                new JaxrsModule(),
                new EventModule(),
                new AgentMainModule(),
                new ConfigurationModule(new ConfigurationFactory(properties)));

        server = injector.getInstance(TestingHttpServer.class);
        agent = injector.getInstance(Agent.class);

        server.start();
        client = new ApacheHttpClient();

        installationHelper = new InstallationHelper();
        appleInstallation = installationHelper.getAppleInstallation();
        bananaInstallation = installationHelper.getBananaInstallation();
    }

    @BeforeMethod
    public void resetState()
    {
        for (Slot slot : agent.getAllSlots()) {
            if (slot.updateStatus().getAssignment() != null) {
                slot.stop();
            }
            agent.terminateSlot(slot.getId());
        }
        assertTrue(agent.getAllSlots().isEmpty());
    }

    @AfterClass
    public void stopServer()
            throws Exception
    {
        if (server != null) {
            server.stop();
        }

        if (tempDir != null) {
            deleteRecursively(tempDir);
        }
        if (installationHelper != null) {
            installationHelper.destroy();
        }

        if (client != null) {
            client.close();
        }
    }

    @Override
    public Agent getAgent()
    {
        return agent;
    }

    @Override
    protected SlotJobStatus createJob(SlotJob slotJob)
    {
        Request request = Request.Builder.preparePost()
                .setUri(urlFor("/v1/agent/job", slotJob.getSlotJobId().toString()))
                .setHeader(CONTENT_TYPE, MediaType.JSON_UTF_8.toString())
                .setBodyGenerator(jsonBodyGenerator(jsonCodec(SlotJob.class), slotJob))
                .build();
        SlotJobStatus jobStatus = client.execute(request, createJsonResponseHandler(jsonCodec(SlotJobStatus.class)));
        return jobStatus;
    }

    @Override
    protected SlotJobStatus updateSlotJobStatus(SlotJob slotJob, SlotJobState currentState)
            throws InterruptedException
    {
        Request request = Request.Builder.prepareGet()
                .setUri(urlFor("/v1/agent/job", slotJob.getSlotJobId().toString()))
                .setHeader(AirshipHeaders.AIRSHIP_CURRENT_STATE, currentState.toString())
                .setHeader(AirshipHeaders.AIRSHIP_MAX_WAIT, new Duration(1, TimeUnit.MINUTES).toString())
                .build();
        SlotJobStatus jobStatus = client.execute(request, createJsonResponseHandler(jsonCodec(SlotJobStatus.class)));
        return jobStatus;
    }

    @Override
    protected Installation getUpgradeInstallation()
    {
        return appleInstallation;
    }

    @Override
    protected Installation getInitialInstallation()
    {
        return bananaInstallation;
    }

    private URI urlFor(String... pathParts)
    {
        HttpUriBuilder builder = uriBuilderFrom(server.getBaseUrl());
        for (String pathPart : pathParts) {
            builder.appendPath(pathPart);
        }
        return builder.build();
    }
}
