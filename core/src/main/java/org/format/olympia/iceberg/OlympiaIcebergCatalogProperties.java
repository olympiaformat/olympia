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
package org.format.olympia.iceberg;

import java.util.Map;
import java.util.Set;
import org.format.olympia.StringMapBased;
import org.format.olympia.relocated.com.google.common.collect.ImmutableSet;
import org.format.olympia.util.PropertyUtil;

public class OlympiaIcebergCatalogProperties implements StringMapBased {

  public static final String STORAGE_TYPE = "storage.type";

  public static final String STORAGE_OPS_PROPERTIES_PREFIX = "storage.ops.";

  public static final String SYSTEM_NAMESPACE_NAME = "system.ns-name";

  public static final String SYSTEM_NAMESPACE_NAME_DEFAULT = "sys";

  public static final String DTXN_PARENT_NAMESPACE_NAME = "dtxn.parent-ns-name";

  public static final String DTXN_PARENT_NAMESPACE_DEFAULT = "dtxns";

  public static final String DTXN_NAMESPACE_PREFIX = "dtxn.prefix";

  public static final String DTXN_NAMESPACE_PREFIX_DEFAULT = "dtxn_";

  public static final Set<String> PROPERTIES =
      ImmutableSet.<String>builder()
          .add(SYSTEM_NAMESPACE_NAME)
          .add(DTXN_PARENT_NAMESPACE_NAME)
          .add(DTXN_NAMESPACE_PREFIX)
          .build();

  private final Map<String, String> propertiesMap;
  private final String systemNamespaceName;
  private final String dtxnParentNamespaceName;
  private final String dtxnNamespacePrefix;

  public OlympiaIcebergCatalogProperties(Map<String, String> properties) {
    this.propertiesMap =
        PropertyUtil.filterProperties(
            properties, k -> k.startsWith(STORAGE_OPS_PROPERTIES_PREFIX) || PROPERTIES.contains(k));
    this.systemNamespaceName =
        PropertyUtil.propertyAsString(
            properties, SYSTEM_NAMESPACE_NAME, SYSTEM_NAMESPACE_NAME_DEFAULT);
    this.dtxnParentNamespaceName =
        PropertyUtil.propertyAsString(
            properties, DTXN_PARENT_NAMESPACE_NAME, DTXN_PARENT_NAMESPACE_DEFAULT);
    this.dtxnNamespacePrefix =
        PropertyUtil.propertyAsString(
            properties, DTXN_NAMESPACE_PREFIX, DTXN_NAMESPACE_PREFIX_DEFAULT);
  }

  public String systemNamespaceName() {
    return systemNamespaceName;
  }

  public String dtxnNamespacePrefix() {
    return dtxnNamespacePrefix;
  }

  public String dtxnParentNamespaceName() {
    return dtxnParentNamespaceName;
  }

  @Override
  public Map<String, String> asStringMap() {
    return propertiesMap;
  }
}
