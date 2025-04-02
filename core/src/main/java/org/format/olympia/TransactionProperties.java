/*
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
package org.format.olympia;

import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.format.olympia.proto.objects.CatalogDef;
import org.format.olympia.proto.objects.IsolationLevel;
import org.format.olympia.relocated.com.google.common.collect.ImmutableSet;
import org.format.olympia.util.PropertyUtil;

public class TransactionProperties implements StringMapBased {

  public static final String ISOLATION_LEVEL = "isolation-level";
  public static final String ID = "id";
  public static final String TTL_MILLIS = "ttl-millis";

  public static final Set<String> PROPERTIES =
      ImmutableSet.<String>builder().add(ISOLATION_LEVEL).add(ID).add(TTL_MILLIS).build();

  private final Map<String, String> properties;
  private final IsolationLevel isolationLevel;
  private final String id;
  private final long ttlMillis;

  public TransactionProperties(CatalogDef catalogDef, Map<String, String> properties) {
    this.properties = PropertyUtil.filterProperties(properties, PROPERTIES::contains);
    this.isolationLevel =
        properties.containsKey(ISOLATION_LEVEL)
            ? IsolationLevel.valueOf(properties.get(ISOLATION_LEVEL).toUpperCase(Locale.ENGLISH))
            : catalogDef.getTxnIsolationLevel();
    // auto generate UUID if not specified
    this.id = PropertyUtil.propertyAsString(properties, ID, UUID.randomUUID().toString());
    this.ttlMillis =
        PropertyUtil.propertyAsLong(properties, TTL_MILLIS, catalogDef.getTxnTtlMillis());
  }

  @Override
  public Map<String, String> asStringMap() {
    return properties;
  }

  public IsolationLevel isolationLevel() {
    return isolationLevel;
  }

  public String txnId() {
    return id;
  }

  public long txnValidMillis() {
    return ttlMillis;
  }
}
