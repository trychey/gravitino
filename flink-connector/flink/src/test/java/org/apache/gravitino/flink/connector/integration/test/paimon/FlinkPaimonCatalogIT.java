package org.apache.gravitino.flink.connector.integration.test.paimon;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.flink.connector.integration.test.FlinkCommonIT;
import org.apache.gravitino.flink.connector.paimon.GravitinoPaimonCatalogFactoryOptions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

public class FlinkPaimonCatalogIT extends FlinkCommonIT {

    private static final String DEFAULT_PAIMON_CATALOG = "test_flink_paimon_filesystem_schema_catalog";

    private static org.apache.gravitino.Catalog catalog;

    @BeforeAll
    static void setup() {
        initPaimonCatalog();
    }

    @AfterAll
    static void stop(){
        Preconditions.checkNotNull(metalake);
        metalake.dropCatalog(DEFAULT_PAIMON_CATALOG, true);
    }

    private static void initPaimonCatalog() {
        Preconditions.checkNotNull(metalake);
        catalog =
                metalake.createCatalog(
                        DEFAULT_PAIMON_CATALOG,
                        org.apache.gravitino.Catalog.Type.RELATIONAL,
                        "paimon",
                        null,
                        ImmutableMap.of(GravitinoPaimonCatalogFactoryOptions.backendType.key(), "jdbc"));
    }


    @Override
    protected Catalog currentCatalog() {
        return catalog;
    }
}
