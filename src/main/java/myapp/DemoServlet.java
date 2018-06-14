/*
 * Copyright 2016 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package myapp;

import com.google.api.gax.core.ExecutorProvider;
import com.google.appengine.repackaged.com.google.common.util.concurrent.MoreExecutors;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import org.joda.time.DateTime;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.logging.Level;
import java.util.logging.Logger;


public class DemoServlet extends HttpServlet {

  private static final String PROJECT_ID = "pubsub-in-gae-te-1528383772593";
  private static final String TOPIC_ID = "test_topic_mj";
  private static final Logger logger = Logger.getLogger(DemoServlet.class.getName());

  @Override
  public void doGet(HttpServletRequest req, HttpServletResponse resp)
          throws IOException {
    resp.setContentType("text/plain");
    resp.getWriter().println("{ \"name\": \"World\" }");

    System.out.println("Project ID:" + PROJECT_ID);

    try {
      String message = DateTime.now().toDateTime().toString();
      publishMessage(message);
      logger.log(Level.INFO, "Successfully published message " + message);
    } catch (Exception ex) {
      System.out.println("Something went wrong when publishing message!" + ex.getMessage());
      logger.log(Level.SEVERE, "Something went wrong when publishing message!" + ex.getMessage());
      for (StackTraceElement el : ex.getStackTrace()) {
        logger.log(Level.SEVERE, el.toString());
      }
    }
  }

  private static class MyThreadFactory implements ThreadFactory {
    @Override
    public Thread newThread(Runnable r) {
      r.run();
      return Thread.currentThread();
    }
  }

  private static class MyExecutorProvider implements ExecutorProvider {
    @Override
    public boolean shouldAutoClose() {
      return true;
    }

    @Override
    public ScheduledExecutorService getExecutor() {
      return MoreExecutors.getExitingScheduledExecutorService(new ScheduledThreadPoolExecutor(1, new MyThreadFactory()));
    }
  }

  private static void publishMessage(String message) throws Exception {
    ProjectTopicName topicName = ProjectTopicName.of(PROJECT_ID, TOPIC_ID);
    Publisher publisher = null;
    try {
      publisher = Publisher.newBuilder(topicName).setExecutorProvider(new MyExecutorProvider()).build();
      ByteString data = ByteString.copyFromUtf8(message);
      PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
              .setData(data)
              .build();

      //schedule a message to be published, messages are automatically batched
      //only publish, don't care for the outcome
      publisher.publish(pubsubMessage);
    } finally {
      if (publisher != null) {
        publisher.shutdown();
      }
    }
  }
}
