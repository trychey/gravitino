/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.aux;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.datastrato.graviton.utils.IsolatedClassLoader;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestGravitonAuxiliaryServiceManager {

  @Test
  public void testGravitonAuxServiceManagerEmptyServiceName() {
    GravitonAuxiliaryServiceManager auxServiceManager = new GravitonAuxiliaryServiceManager();
    auxServiceManager.serviceInit(
        ImmutableMap.of(GravitonAuxiliaryServiceManager.AUX_SERVICE_NAMES, ""));
    auxServiceManager.serviceStart();
    auxServiceManager.serviceStop();
  }

  @Test
  public void testGravitonAuxServiceNotSetClassPath() {
    GravitonAuxiliaryServiceManager auxServiceManager = new GravitonAuxiliaryServiceManager();
    Assertions.assertThrowsExactly(
        IllegalArgumentException.class,
        () ->
            auxServiceManager.serviceInit(
                ImmutableMap.of(GravitonAuxiliaryServiceManager.AUX_SERVICE_NAMES, "mock1")));
  }

  @Test
  public void testGravitonAuxServiceManager() throws Exception {
    GravitonAuxiliaryService auxService = mock(GravitonAuxiliaryService.class);
    GravitonAuxiliaryService auxService2 = mock(GravitonAuxiliaryService.class);

    IsolatedClassLoader isolatedClassLoader =
        new IsolatedClassLoader(
            Collections.emptyList(), Collections.emptyList(), Collections.emptyList());

    GravitonAuxiliaryServiceManager auxServiceManager = new GravitonAuxiliaryServiceManager();
    GravitonAuxiliaryServiceManager spyAuxManager = spy(auxServiceManager);

    doReturn(isolatedClassLoader).when(spyAuxManager).getIsolatedClassLoader(anyString());
    doReturn(auxService).when(spyAuxManager).loadAuxService("mock1", isolatedClassLoader);
    doReturn(auxService2).when(spyAuxManager).loadAuxService("mock2", isolatedClassLoader);

    spyAuxManager.serviceInit(
        ImmutableMap.of(
            GravitonAuxiliaryServiceManager.AUX_SERVICE_NAMES,
            "mock1,mock2",
            "mock1." + GravitonAuxiliaryServiceManager.AUX_SERVICE_CLASSPATH,
            "/tmp",
            "mock2." + GravitonAuxiliaryServiceManager.AUX_SERVICE_CLASSPATH,
            "/tmp"));
    verify(auxService, times(1)).serviceInit(any());
    verify(auxService2, times(1)).serviceInit(any());

    spyAuxManager.serviceStart();
    verify(auxService, times(1)).serviceStart();
    verify(auxService2, times(1)).serviceStart();

    spyAuxManager.serviceStop();
    verify(auxService, times(1)).serviceStop();
    verify(auxService2, times(1)).serviceStop();
  }
}
