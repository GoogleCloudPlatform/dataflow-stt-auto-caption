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

import com.google.solutions.df.stt.autocaption.common.OutputProcessorTransform;
import com.google.solutions.df.stt.autocaption.common.ReadAudioFileTransform;
import com.google.solutions.df.stt.autocaption.common.STTAutoCaptionTransform;
import com.google.solutions.df.stt.autocaption.common.Util;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Duration;

public class STTAutoCaptionPipeline {
  /** Default window interval to create side inputs for header records. */
  private static final Duration WINDOW_INTERVAL = Duration.standardSeconds(10);

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
                .build()).setCoder(RowCoder.of(Util.outputSchema))
        .apply(
            "FixedWindow",
            Window.<Row>into(FixedWindows.of(WINDOW_INTERVAL))
                .triggering(
                    Repeatedly.forever(
                        AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.ZERO)))
                .discardingFiredPanes()
                .withAllowedLateness(Duration.ZERO))        
        .apply(
            "OutputData",
            OutputProcessorTransform.newBuilder()
            .setTopicId(options.getOutputTopic()).build());
    p.run();
  }
}
