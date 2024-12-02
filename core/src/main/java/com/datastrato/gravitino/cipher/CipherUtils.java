/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.cipher;

import com.datastrato.gravitino.Configs;
import com.datastrato.gravitino.GravitinoEnv;
import com.xiaomi.keycenter.KeycenterHelper;
import com.xiaomi.keycenter.common.iface.DataProtectionException;
import com.xiaomi.keycenter.detection.CipherDetection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CipherUtils {
  private static final Logger LOG = LoggerFactory.getLogger(CipherUtils.class);

  private static final String KEY_CENTER_SID_VALUE =
      GravitinoEnv.getInstance().config() != null
          ? GravitinoEnv.getInstance().config().get(Configs.KEY_CENTER_SID)
          : Configs.DEFAULT_KEY_CENTER_SID;

  private CipherUtils() {}

  public static String decryptStringWithoutCompress(String encryptedString) {
    try {
      boolean isEncryptedString = CipherDetection.isKeycenterCipher(encryptedString);
      if (isEncryptedString) {
        return KeycenterHelper.decryptNoCompress(KEY_CENTER_SID_VALUE, encryptedString);
      }
    } catch (DataProtectionException e) {
      LOG.error("While decrypting string using keycenter happened exception: ", e);
      throw new RuntimeException(e);
    }
    return encryptedString;
  }
}
