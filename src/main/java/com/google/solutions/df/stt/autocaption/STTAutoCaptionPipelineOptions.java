/*
 * Copyright 2020 Google LLC
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
package com.google.solutions.df.stt.autocaption;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface STTAutoCaptionPipelineOptions extends PipelineOptions {

  @Description("Pub/Sub subscription ID to receive input Cloud Storage notifications from")
  String getInputNotificationSubscription();

  void setInputNotificationSubscription(String value);

  @Description("Pub/Sub topic ID to publish the results to")
  String getOutputTopic();

  void setOutputTopic(String value);

  @Description("Minimum Stability Level")
  @Default.Double(0.8)
  Double getStabilityThreshold();

  void setStabilityThreshold(Double value);

  @Description("Maximum Word Limit")
  @Default.Integer(22)
  Integer getWordCount();

  void setWordCount(Integer value);
}
