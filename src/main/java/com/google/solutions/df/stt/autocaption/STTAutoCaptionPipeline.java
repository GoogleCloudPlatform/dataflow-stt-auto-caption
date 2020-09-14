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

import com.google.solutions.df.stt.autocaption.common.ReadAudioFileTransform;
import com.google.solutions.df.stt.autocaption.common.STTAutoCaptionTransform;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

public class STTAutoCaptionPipeline {
  public static void main(String[] args) {

    STTAutoCaptionPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(STTAutoCaptionPipelineOptions.class);
    run(options);
  }

  private static void run(STTAutoCaptionPipelineOptions options) {
    Pipeline p = Pipeline.create(options);
    p.apply(
            "ReadClip",
            ReadAudioFileTransform.newBuilder()
                .setSubscriptionId(options.getInputNotificationSubscription())
                .build())
        .apply(
            "ProcessClip",
            STTAutoCaptionTransform.newBuilder()
                .setMaxWordCount(options.getWordCount())
                .setStabilityThreshold(options.getStabilityThreshold())
                .build());
    p.run();
  }
}
