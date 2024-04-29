/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.cipher;

import com.xiaomi.keycenter.KeycenterHelper;
import com.xiaomi.keycenter.common.iface.DataProtectionException;
import com.xiaomi.keycenter.detection.CipherDetection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CipherUtils {
  private static final Logger LOG = LoggerFactory.getLogger(CipherUtils.class);

  private CipherUtils() {}

  public static String decryptStringWithoutCompress(String encryptedString) {
    try {
      boolean isEncryptedString = CipherDetection.isKeycenterCipher(encryptedString);
      if (isEncryptedString) {
        return KeycenterHelper.decryptNoCompress(
            KeycenterConstants.KEY_CENTER_SID, encryptedString);
      }
    } catch (DataProtectionException e) {
      LOG.error("While decrypting string using keycenter happened exception: ", e);
      throw new RuntimeException(e);
    }
    return encryptedString;
  }
}
