/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.rel;

import com.datastrato.graviton.rel.Distribution.DistributionMethod;
import com.datastrato.graviton.rel.transforms.Transform;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestDistribution {

  @Test
  void testDistribution() {
    Distribution.DistributionBuilder builder = new Distribution.DistributionBuilder();
    builder.withdistributionMethod(DistributionMethod.HASH);
    builder.withDistributionNumber(1);
    builder.withTransforms(new Transform[] {});
    Distribution bucket = builder.build();

    Assertions.assertEquals(DistributionMethod.HASH, bucket.distMethod());
    Assertions.assertEquals(1, bucket.distributionNumber());
    Assertions.assertArrayEquals(new Transform[] {}, bucket.transforms());

    builder.withdistributionMethod(DistributionMethod.EVEN);
    builder.withDistributionNumber(11111);
    builder.withTransforms(new Transform[] {});
    bucket = builder.build();

    Assertions.assertEquals(DistributionMethod.EVEN, bucket.distMethod());
    Assertions.assertEquals(11111, bucket.distributionNumber());
    Assertions.assertArrayEquals(new Transform[] {}, bucket.transforms());
  }
}
