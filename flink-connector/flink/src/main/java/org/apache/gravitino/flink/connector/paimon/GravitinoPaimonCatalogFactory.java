package org.apache.gravitino.flink.connector.paimon;

import com.google.common.collect.ImmutableSet;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.gravitino.flink.connector.hive.GravitinoHiveCatalog;
import org.apache.paimon.flink.FlinkCatalog;
import org.apache.paimon.flink.FlinkCatalogFactory;

import java.util.Collections;
import java.util.Set;

/**
 * Factory for creating instances of {@link GravitinoPaimonCatalog}. It will be created by SPI
 * discovery in Flink.
 */
public class GravitinoPaimonCatalogFactory implements CatalogFactory {

    @Override
    public Catalog createCatalog(Context context) {
        FlinkCatalog catalog = new FlinkCatalogFactory().createCatalog(context);
        return new GravitinoPaimonCatalog(context.getName(),catalog);
    }

    @Override
    public String factoryIdentifier() {
        return GravitinoPaimonCatalogFactoryOptions.IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return ImmutableSet.of(GravitinoPaimonCatalogFactoryOptions.backendType);
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Collections.emptySet();
    }
}
