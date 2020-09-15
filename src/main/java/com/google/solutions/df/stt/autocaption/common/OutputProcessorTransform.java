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
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.schemas.transforms.Group;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("serial")
@AutoValue
public abstract class OutputProcessorTransform extends PTransform<PCollection<Row>, PDone> {
  private static final Logger LOG = LoggerFactory.getLogger(OutputProcessorTransform.class);

  public abstract String topicId();

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setTopicId(String topic);

    public abstract OutputProcessorTransform build();
  }

  public static Builder newBuilder() {
    return new AutoValue_OutputProcessorTransform.Builder();
  }

  @Override
  public PDone expand(PCollection<Row> input) {

    return input
        .apply("GroupByKeyField", Group.byFieldNames("file_name"))
        .apply("VTTConvert", MapElements.via(new MergeFilterResponse()))
        .setCoder(RowCoder.of(Util.webVttMetadataSchema))
        .apply(
            "ConvertToPubSubMessage",
            ParDo.of(
                new DoFn<Row, PubsubMessage>() {

                  @ProcessElement
                  public void processContext(ProcessContext c) {

                    Row vttRow = c.element();
                    StringBuilder builder = new StringBuilder();
                    String fileName = vttRow.getString("file_name");
                    Collection<Row> transcriptDataList = vttRow.getArray("transcript_data");
                    transcriptDataList.forEach(
                        transcript -> {
                          builder
                              .append(transcript.getString("time_offset"))
                              .append("\n")
                              .append(transcript.getString("transcript"))
                              .append("\n");
                        });

                    LOG.info(builder.toString());
                    c.output(
                        new PubsubMessage(
                            builder.toString().getBytes(), ImmutableMap.of("file_name", fileName)));
                  }
                }))
        .apply("PublishToPubSub", PubsubIO.writeMessages().to(topicId()));
  }

  public static class MergeFilterResponse extends SimpleFunction<Row, Row> {
    @Override
    public Row apply(Row input) {
      String fileName = input.getRow("key").getString("file_name");
      List<Row> transcripts = new ArrayList<Row>();
      Iterable<Row> values = input.getIterable("value");
      Objects.requireNonNull(values)
          .forEach(
              v -> {
                String timeOffset =
                    String.format(
                        "%s%s%s",
                        Util.formatSecondField(v.getInt64("start_time_offset")),
                        "-->",
                        Util.formatSecondField(v.getInt64("end_time_offset")));
                Row webVttData =
                    Row.withSchema(Util.webVttSchema)
                        .addValues(timeOffset, v.getString("transcript"))
                        .build();
                transcripts.add(webVttData);
              });
      Row row = Row.withSchema(Util.webVttMetadataSchema).addValues(fileName, transcripts).build();
      LOG.debug("Output Row {}", row.toString());
      return row;
    }
  }
}
