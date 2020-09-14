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

import static org.apache.beam.sdk.schemas.Schema.toSchema;

import java.util.stream.Stream;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;

public class Util {
  static final Schema outputSchema =
      Stream.of(
              Schema.Field.of("file_name", FieldType.STRING).withNullable(true),
              Schema.Field.of("start_time_offset", FieldType.INT64).withNullable(true),
              Schema.Field.of("end_time_offset", FieldType.INT64).withNullable(true),
              Schema.Field.of("transcript", FieldType.STRING).withNullable(true),
              Schema.Field.of("stability", FieldType.DOUBLE).withNullable(true),
              Schema.Field.of("confidence", FieldType.DOUBLE).withNullable(true),
              Schema.Field.of("is_final", FieldType.BOOLEAN).withNullable(true),
              Schema.Field.of("word_count", FieldType.INT32).withNullable(true))
          .collect(toSchema());
}
