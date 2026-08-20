package io.numaproj.kafka.config;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class UserConfigTest {

  @Test
  void getOnError_builtViaBuilder_defaultsToFail() {
    assertEquals(OnError.FAIL, UserConfig.builder().build().getOnError());
  }

  @Test
  void getOnError_builtWithoutTheBuilder_stillDefaultsToFail() {
    // @Builder.Default does not apply to the no-args constructor, which leaves the field null.
    assertEquals(OnError.FAIL, new UserConfig().getOnError());
  }

  @Test
  void getOnError_setExplicitly_returnsIt() {
    assertEquals(OnError.SKIP, UserConfig.builder().onError(OnError.SKIP).build().getOnError());
  }
}
