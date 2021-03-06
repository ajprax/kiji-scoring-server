/**
 * (c) Copyright 2013 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
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

package org.kiji.lifecycle

import org.kiji.modeling.Extractor
import org.kiji.express.flow.FlowCell
import org.kiji.modeling.Scorer

/**
 * Simple implementation of the extractor and scorer phases of the model lifecycle. This simply
 * take the input tuple and returns the string length, unless a freshener parameter called
 * 'jennyanydots' is set, in which case the length of its value is returned instead.
 */
class DummyExtractorScorer extends Extractor with Scorer {
  override val extractFn = extract('email_address -> 'word) { line: Seq[FlowCell[CharSequence]] =>
    line.head.datum
  }

  override val scoreFn = score('word) { line: CharSequence =>
    keyValueStore("freshener_parameters").getOrElse("jennyanydots", line).length
  }
}
