package com.datastrato.graviton;

import com.datastrato.graviton.rel.SupportsSchemas;
import com.datastrato.graviton.rel.TableCatalog;
import java.util.Map;

public interface Catalog extends Auditable {

  enum Type {
    RELATIONAL, // Catalog Type for Relational Data Structure, like db.table, catalog.db.table.
    FILE, // Catalog Type for File System (including HDFS, S3, etc.), like path/to/file
    STREAM, // Catalog Type for Streaming Data, like kafka://topic
  }

  /**
   * A reserved property to specify the provider of the catalog. The provider is a string used to
   * specify the class to create the catalog. The provider can be a short name if the catalog is a
   * built-in catalog, or it can be a full qualified class name if the catalog is a custom one.
   *
   * <p>For example, the provider of a built-in Hive catalog could be "hive", Graviton will figure
   * out the related class to create the catalog.
   */
  String PROPERTY_PROVIDER = "provider";

  /**
   * A reserved property to specify the package location of the catalog. The "package" is a string
   * of path to the folder where all the catalog related dependencies is located. The dependencies
   * under the "package" will be loaded by Graviton to create the catalog.
   *
   * <p>The property "package" is not needed if the catalog is a built-in one, Graviton will search
   * the proper location using "provider" to load the dependencies. Only when the folder is in
   * different location, the "package" property is needed.
   */
  String PROPERTY_PACKAGE = "package";

  /** The name of the catalog. */
  String name();

  /** The type of the catalog. */
  Type type();

  /**
   * The comment of the catalog. Note. this method will return null if the comment is not set for
   * this catalog.
   */
  String comment();

  /**
   * The properties of the catalog. Note, this method will return null if the properties are not
   * set.
   */
  Map<String, String> properties();

  /**
   * Return the {@link SupportsSchemas} if the catalog supports schema operations.
   *
   * @throws UnsupportedOperationException if the catalog does not support schema operations.
   */
  default SupportsSchemas asSchemas() throws UnsupportedOperationException {
    throw new UnsupportedOperationException("Catalog does not support schema operations");
  }

  /**
   * return the {@link TableCatalog} if the catalog supports table operations.
   *
   * @throws UnsupportedOperationException if the catalog does not support table operations.
   */
  default TableCatalog asTableCatalog() throws UnsupportedOperationException {
    throw new UnsupportedOperationException("Catalog does not support table operations");
  }
}
