/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.broker.it.workflow;

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import io.zeebe.broker.it.ClientRule;
import io.zeebe.broker.it.EmbeddedBrokerRule;
import io.zeebe.broker.it.startup.BrokerRestartTest;
import io.zeebe.client.WorkflowsClient;
import io.zeebe.client.event.WorkflowInstanceEvent;
import io.zeebe.test.util.TestFileUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

/**
 *
 */
public class LargeWorkflowTest
{
    public static final int CREATION_TIMES = 10_000_000;
    public static final URL PATH = LargeWorkflowTest.class.getResource("");

    public EmbeddedBrokerRule brokerRule = new EmbeddedBrokerRule(() -> brokerConfig(PATH.getPath()));
    public ClientRule clientRule = new ClientRule();
    //    public TopicEventRecorder eventRecorder = new TopicEventRecorder(clientRule);

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule(brokerRule)
                                          .around(clientRule);
    //        .around(eventRecorder);

    //    @Rule
    //    public ExpectedException exception = ExpectedException.none();

    protected static InputStream brokerConfig(final String path)
    {
        final String canonicallySeparatedPath = path.replaceAll(Pattern.quote(File.separator), "/");

        return TestFileUtil.readAsTextFileAndReplace(BrokerRestartTest.class.getClassLoader()
                                                                            .getResourceAsStream("persistent-broker.cfg.toml"), StandardCharsets.UTF_8,
                                                     Collections.singletonMap("\\$\\{brokerFolder\\}", canonicallySeparatedPath));
    }

    @Before
    public void deployProcess()
    {
        final WorkflowsClient workflowService = clientRule.workflows();

        workflowService.deploy(clientRule.getDefaultTopic())
                       .addResourceFromClasspath("workflows/extended-order-process.bpmn")
                       .execute();
    }

    @Test
    public void shouldCreateBunchOfWorkflowInstances()
    {
        final WorkflowsClient workflowService = clientRule.workflows();

        // when
        for (int i = 0; i < CREATION_TIMES; i++)
        {
            workflowService.create(clientRule.getDefaultTopic())
                           .bpmnProcessId("extended-order-process")
                           .latestVersion()
                           .payload("{ \"orderId\": 31243, \"orderStatus\": \"NEW\", \"orderItems\": [435, 182, 376] }")
                           .execute();

            if (i % 1_000 == 0)
            {
                System.out.println("Iteration: " + i);
            }
        }
    }

    @Test
    public void shouldCreateBunchOfWorkflowInstancesAsync()
    {
        final WorkflowsClient workflowService = clientRule.workflows();

        final int batchSize = 1000;
        final List<Future<WorkflowInstanceEvent>> requests = new ArrayList<>(batchSize);
        final AtomicLong finished = new AtomicLong(0);
        final AtomicLong failed = new AtomicLong(0);

        final Supplier<Void> pollRequests = () ->
        {
            final Iterator<Future<WorkflowInstanceEvent>> iterator = requests.iterator();
            while (iterator.hasNext())
            {
                final Future<WorkflowInstanceEvent> request = iterator.next();
                if (request.isDone())
                {
                    iterator.remove();
                    finished.incrementAndGet();
                    try
                    {
                        final WorkflowInstanceEvent workflowInstanceEvent = request.get();
                        if (!workflowInstanceEvent.getState().equals("WORKFLOW_INSTANCE_CREATED"))
                        {
                            failed.incrementAndGet();
                            System.out.println("Failed to create workflow instance: " + workflowInstanceEvent);
                        }
                    }
                    catch (final Exception e)
                    {
                        failed.incrementAndGet();
                        System.out.println("Failed to create workflow instance: " + e.getMessage());
                        e.printStackTrace();
                    }

                    if (finished.get() % 1000 == 0)
                    {
                        System.out.println("Finished requests: " + finished);
                    }
                }
            }

            return null;
        };

        final BooleanSupplier enoughRequests = () -> requests.size() + finished.get() == CREATION_TIMES;

        while (!enoughRequests.getAsBoolean())
        {
            while (!enoughRequests.getAsBoolean() && requests.size() < batchSize)
            {
                final Future<WorkflowInstanceEvent> request = workflowService.create(clientRule.getDefaultTopic())
                                                                             .bpmnProcessId("extended-order-process")
                                                                             .latestVersion()
                                                                             .payload(
                                                                           "{ \"orderId\": 31243, \"orderStatus\": \"NEW\", \"orderItems\": [435, 182, 376] }")
                                                                             .executeAsync();

                requests.add(request);
            }

            pollRequests.get();

        }

        while (!requests.isEmpty())
        {
            pollRequests.get();
        }

        System.out.println("Done (finished: " + finished + ", failed: " + failed + ")");
    }

    @Test
    public void shouldCreateAndCompleteWorkflowInstances() throws InterruptedException
    {
        final WorkflowsClient workflowService = clientRule.workflows();

        final AtomicLong completed = new AtomicLong(0);

        clientRule.tasks()
                  .newTaskSubscription(clientRule.getDefaultTopic())
                  .taskType("reserveOrderItems")
                  .lockOwner("stocker")
                  .lockTime(Duration.ofMinutes(5))
                  .handler((tasksClient, task) -> {
                      final long c = completed.incrementAndGet();
                      tasksClient.complete(task)
                                 .payload("{ \"orderStatus\": \"RESERVED\" }")
                                 .execute();
                      if (c % 100 == 0)
                      {
                          System.out.println("Completed: " + c);
                      }
                  })
                  .open();

        // when
        for (int i = 0; i < 100_000; i++)
        {
            workflowService.create(clientRule.getDefaultTopic())
                           .bpmnProcessId("extended-order-process")
                           .latestVersion()
                           .payload("{ \"orderId\": 31243, \"orderStatus\": \"NEW\", \"orderItems\": [435, 182, 376] }")
                           .executeAsync();
        }

        // then

        while (completed.get() < 1_000_000)
        {
            Thread.sleep(1000);
        }
    }
}
