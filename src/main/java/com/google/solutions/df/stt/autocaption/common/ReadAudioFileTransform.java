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

import com.google.auto.value.AutoValue;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("serial")
@AutoValue
public abstract class ReadAudioFileTransform
    extends PTransform<PBegin, PCollection<KV<String, ByteString>>> {

  private static final Logger LOG = LoggerFactory.getLogger(ReadAudioFileTransform.class);

  private final Counter numberOfFiles =
      Metrics.counter(ReadAudioFileTransform.class, "numberOfFiles");
  private static final String OBJECT_FINALIZE = "OBJECT_FINALIZE";
  // Allowed audio file extensions supported by the STT API
  private static final String FILE_PATTERN = "(^.*\\.(?i)(wav|WAV)$)";
  // 5000000,705000
  private static final Integer CHUNK_SIZE = 5000000;

  public abstract String subscriptionId();

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setSubscriptionId(String subscriptionId);

    public abstract ReadAudioFileTransform build();
  }

  public static Builder newBuilder() {
    return new AutoValue_ReadAudioFileTransform.Builder();
  }

  @Override
  public PCollection<KV<String, ByteString>> expand(PBegin input) {
    return input
        .apply(
            "ReadFileMetadataFromPubSubMessage",
            PubsubIO.readMessagesWithAttributes().fromSubscription(subscriptionId()))
        .apply("ValidateEventType", ParDo.of(new ValidateEventType()))
        .apply("FindFile", FileIO.matchAll().withEmptyMatchTreatment(EmptyMatchTreatment.ALLOW))
        .apply(FileIO.readMatches())
        .apply("ValidateFileExtension", ParDo.of(new ValidateFileExtension()))
        .apply("ReadFile", ParDo.of(new ReadFile(CHUNK_SIZE)));
  }

  public static class ReadFile extends DoFn<KV<String, ReadableFile>, KV<String, ByteString>> {

    private Integer chunkSize;

    public ReadFile(Integer chunkSize) {
      this.chunkSize = chunkSize;
    }

    @ProcessElement
    public void processElement(ProcessContext c, RestrictionTracker<OffsetRange, Long> tracker)
        throws IOException {
      String fileName = c.element().getKey();
      try (SeekableByteChannel channel = getReader(c.element().getValue())) {
        ByteBuffer readBuffer = ByteBuffer.allocate(chunkSize);
        ByteString buffer = ByteString.EMPTY;
        for (long i = tracker.currentRestriction().getFrom(); tracker.tryClaim(i); ++i) {
          long startOffset = (i * chunkSize) - chunkSize;
          channel.position(startOffset);
          readBuffer = ByteBuffer.allocate(chunkSize);
          buffer = ByteString.EMPTY;
          channel.read(readBuffer);
          readBuffer.flip();
          buffer = ByteString.copyFrom(readBuffer);
          readBuffer.clear();
          LOG.debug(
              "Current Restriction {}, Content Size{}",
              tracker.currentRestriction(),
              buffer.size());
          String key = String.format("%s%s%s", fileName, "~", i);
          c.outputWithTimestamp(KV.of(key, buffer), Instant.now());
        }
      }
    }
    // [END loadSnippet_1]
    @GetInitialRestriction
    public OffsetRange getInitialRestriction(@Element KV<String, ReadableFile> file)
        throws IOException {
      long totalBytes = file.getValue().getMetadata().sizeBytes();
      long totalSplit = 0;
      if (totalBytes < chunkSize) {
        totalSplit = 2;
      } else {
        totalSplit = totalSplit + (totalBytes / chunkSize);
        long remaining = totalBytes % chunkSize;
        if (remaining > 0) {
          totalSplit = totalSplit + 2;
        }
      }
      LOG.debug(
          "File Read Transform:ReadFile: Total Bytes {} for File {} -Initial Restriction range from 1 to: {}. Batch size of each chunk: {} ",
          totalBytes,
          file.getKey(),
          totalSplit,
          chunkSize);
      return new OffsetRange(1, totalSplit);
    }

    @SplitRestriction
    public void splitRestriction(
        @Element KV<String, ReadableFile> file,
        @Restriction OffsetRange range,
        OutputReceiver<OffsetRange> out) {
      for (final OffsetRange p : range.split(1, 1)) {
        out.output(p);
      }
    }

    @NewTracker
    public OffsetRangeTracker newTracker(@Restriction OffsetRange range) {
      return new OffsetRangeTracker(new OffsetRange(range.getFrom(), range.getTo()));
    }

    private static SeekableByteChannel getReader(ReadableFile eventFile) {
      SeekableByteChannel channel = null;
      try {
        channel = eventFile.openSeekable();
      } catch (IOException e) {
        LOG.error("Failed to Open File {}", e.getMessage());
        throw new RuntimeException(e);
      }
      return channel;
    }
  }

  public class ValidateEventType extends DoFn<PubsubMessage, String> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      String bucket = c.element().getAttribute("bucketId");
      String object = c.element().getAttribute("objectId");
      String eventType = c.element().getAttribute("eventType");
      GcsPath uri = GcsPath.fromComponents(bucket, object);
      if (eventType != null && eventType.equalsIgnoreCase(OBJECT_FINALIZE)) {
        String path = uri.toString();
        numberOfFiles.inc();
        c.output(path);
      } else {
        LOG.debug("Event Type Not Supported {}", eventType);
      }
    }
  }

  public static class ValidateFileExtension extends DoFn<ReadableFile, KV<String, ReadableFile>> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      ReadableFile file = c.element();
      String fileName = file.getMetadata().resourceId().toString();
      if (fileName.matches(FILE_PATTERN)) {
        c.output(KV.of(fileName, file));
        LOG.debug("Valid filename: `{}`", fileName);
      } else {
        LOG.error("Invalid filename: `{}`", fileName);
      }
    }
  }
}
