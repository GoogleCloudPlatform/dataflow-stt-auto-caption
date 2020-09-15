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
package com.google.solutions.df.stt.autocaption.common;

import com.google.api.gax.rpc.ApiStreamObserver;
import com.google.api.gax.rpc.BidiStreamingCallable;
import com.google.auto.value.AutoValue;
import com.google.cloud.speech.v1.RecognitionConfig;
import com.google.cloud.speech.v1.RecognitionConfig.AudioEncoding;
import com.google.cloud.speech.v1.SpeechClient;
import com.google.cloud.speech.v1.StreamingRecognitionConfig;
import com.google.cloud.speech.v1.StreamingRecognizeRequest;
import com.google.cloud.speech.v1.StreamingRecognizeResponse;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.lang3.ObjectUtils;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("serial")
@AutoValue
public abstract class STTAutoCaptionTransform
    extends PTransform<PCollection<KV<String, ByteString>>, PCollection<Row>> {

  private static final Logger LOG = LoggerFactory.getLogger(STTAutoCaptionTransform.class);
  private static final Integer WINDOW_INTERVAL = 2;

  public abstract Double stabilityThreshold();

  public abstract Integer maxWordCount();

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setStabilityThreshold(Double stability);

    public abstract Builder setMaxWordCount(Integer wordCount);

    public abstract STTAutoCaptionTransform build();
  }

  public static Builder newBuilder() {
    return new AutoValue_STTAutoCaptionTransform.Builder();
  }

  @Override
  public PCollection<Row> expand(PCollection<KV<String, ByteString>> input) {
    return input
        .apply("RequestHandler", ParDo.of(new STTApiProcessor(stabilityThreshold())))
        .setCoder(KvCoder.of(StringUtf8Coder.of(), RowCoder.of(Util.outputSchema)))
        .apply("ResponseHandler", ParDo.of(new STTResultsHandler(maxWordCount())))
        .setRowSchema(Util.outputSchema);
  }

  class STTResultsHandler extends DoFn<KV<String, Row>, Row> {

    private Integer maxWordCount;

    @StateId("startOffset")
    private final StateSpec<ValueState<Long>> startOffset = StateSpecs.value();

    @StateId("lastEmitWordCount")
    private final StateSpec<ValueState<Integer>> lastEmitWordCount = StateSpecs.value();

    @StateId("outputRow")
    private final StateSpec<ValueState<Row>> outputRow =
        StateSpecs.value(RowCoder.of(Util.outputSchema));

    @TimerId("expiry")
    private final TimerSpec expirySpec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

    public STTResultsHandler(Integer maxWordCount) {
      this.maxWordCount = maxWordCount;
    }

    @ProcessElement
    public void process(
        @Element KV<String, Row> element,
        @StateId("startOffset") ValueState<Long> startOffset,
        @StateId("lastEmitWordCount") ValueState<Integer> lastEmitWordCount,
        @StateId("outputRow") ValueState<Row> outputRow,
        @TimerId("expiry") Timer expiry,
        OutputReceiver<Row> output) {

      expiry.offset(Duration.standardSeconds(WINDOW_INTERVAL)).setRelative();
      Row apiResult = element.getValue();
      long startTime = ObjectUtils.firstNonNull(startOffset.read(), 0L);
      int lastWordCount = ObjectUtils.firstNonNull(lastEmitWordCount.read(), 0);
      // determine emit or not
      String displayText =
          Util.transcriptConstruct(apiResult.getString("transcript"), lastWordCount);
      int currentWordCount = Util.splitWord(displayText).size();
      boolean emitResult = currentWordCount >= maxWordCount;
      Row displayRow =
          Row.fromRow(apiResult)
              .withFieldValue("transcript", displayText)
              .withFieldValue("start_time_offset", startTime)
              .build();
      if (emitResult) {
        LOG.debug("*******Display Row Count Emit {}********", displayRow.toString());
        startOffset.write(apiResult.getInt64("end_time_offset"));
        lastEmitWordCount.write(currentWordCount + lastWordCount);
        output.output(displayRow);

      } else {
        outputRow.write(displayRow);
      }
    }

    @OnTimer("expiry")
    public void onExpiry(
        OnTimerContext context,
        @StateId("startOffset") ValueState<Long> startOffset,
        @StateId("lastEmitWordCount") ValueState<Integer> lastEmitWordCount,
        @StateId("outputRow") ValueState<Row> outputRow,
        OutputReceiver<Row> output) {

      Row remainingTranscript = outputRow.read();
      LOG.debug("*******Display Row Time Emit {}********", remainingTranscript.toString());
      output.output(remainingTranscript);
    }
  }

  class STTApiProcessor extends DoFn<KV<String, ByteString>, KV<String, Row>> {

    private Double stability;
    private SpeechClient speechClient;
    private RecognitionConfig recConfig;
    private StreamingRecognitionConfig config;

    public STTApiProcessor(Double stability) {
      this.stability = stability;
    }

    @Setup
    public void setup() throws IOException {
      speechClient = SpeechClient.create();
      recConfig =
          RecognitionConfig.newBuilder()
              .setEncoding(AudioEncoding.LINEAR16)
              .setLanguageCode("en-US")
              .setSampleRateHertz(44100)
              .setModel("video")
              .setUseEnhanced(true)
              .setEnableWordTimeOffsets(true)
              .setUseEnhanced(true)
              .setEnableAutomaticPunctuation(true)
              .setMaxAlternatives(1)
              .build();
      config =
          StreamingRecognitionConfig.newBuilder()
              .setConfig(recConfig)
              .setInterimResults(true)
              .build();
    }

    @Teardown
    public void teardown() {
      if (speechClient != null) {
        speechClient.close();
      }
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws InterruptedException, ExecutionException {

      String[] fileMetadata = c.element().getKey().split("\\~");
      LOG.debug("File name {}, split {}", fileMetadata[0], fileMetadata[1]);
      ResponseApiStreamingObserver<StreamingRecognizeResponse> responseObserver =
          new ResponseApiStreamingObserver<>();

      BidiStreamingCallable<StreamingRecognizeRequest, StreamingRecognizeResponse> callable =
          speechClient.streamingRecognizeCallable();

      @SuppressWarnings("deprecation")
      ApiStreamObserver<StreamingRecognizeRequest> requestObserver =
          callable.bidiStreamingCall(responseObserver);
      // The first request must **only** contain the audio configuration:
      requestObserver.onNext(
          StreamingRecognizeRequest.newBuilder().setStreamingConfig(config).build());

      // Subsequent requests must **only** contain the audio data.
      requestObserver.onNext(
          StreamingRecognizeRequest.newBuilder().setAudioContent(c.element().getValue()).build());
      requestObserver.onCompleted();

      List<StreamingRecognizeResponse> responses = responseObserver.future().get();

      for (StreamingRecognizeResponse response : responses) {
        if (!response.hasError()) {

          response
              .getResultsList()
              .forEach(
                  result -> {
                    result
                        .getAlternativesList()
                        .forEach(
                            alternative -> {
                              boolean emitRow =
                                  (result.getStability() >= stability)
                                      && alternative.getTranscript().length() > 0;
                              if (emitRow) {
                                Row outputRow =
                                    Row.withSchema(Util.outputSchema)
                                        .addValues(
                                            fileMetadata[0],
                                            0L,
                                            result.getResultEndTime().getSeconds()
                                                * Long.valueOf(fileMetadata[1]),
                                            alternative.getTranscript(),
                                            (double) result.getStability(),
                                            (double) alternative.getConfidence(),
                                            result.getIsFinal(),
                                            alternative.getWordsCount())
                                        .build();
                                LOG.debug("****Output Row {}****", outputRow);

                                c.output(KV.of(fileMetadata[0], outputRow));
                              }
                            });
                  });
        } else {
          LOG.error("Error {}", response.getError());
        }
      }
    }
  }

  private class ResponseApiStreamingObserver<T> implements ApiStreamObserver<T> {
    private final SettableFuture<List<T>> future = SettableFuture.create();
    private final List<T> messages = new java.util.ArrayList<T>();

    @Override
    public void onNext(T message) {
      messages.add(message);
    }

    @Override
    public void onError(Throwable t) {
      future.setException(t);
    }

    @Override
    public void onCompleted() {
      future.set(messages);
    }

    // Returns the SettableFuture object to get received messages / exceptions.
    public SettableFuture<List<T>> future() {
      return future;
    }
  }
}
