package io.numaproj.kafka.encryption.aws;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DekCredentialsProviderTest {

  @Mock private AutoCloseable ownedProvider;
  @Mock private AutoCloseable ownedStsClient;

  @Test
  void closeReleasesStsClientThenProvider() throws Exception {
    new DekCredentialsProvider(null, ownedProvider, ownedStsClient).close();

    InOrder inOrder = inOrder(ownedStsClient, ownedProvider);
    inOrder.verify(ownedStsClient).close();
    inOrder.verify(ownedProvider).close();
  }

  @Test
  void closeContinuesWhenOneResourceFails() throws Exception {
    doThrow(new RuntimeException("boom")).when(ownedStsClient).close();

    new DekCredentialsProvider(null, ownedProvider, ownedStsClient).close(); // must not throw

    verify(ownedProvider).close();
  }

  @Test
  void defaultChainOwnsNothing() {
    DekCredentialsProvider creds = DekCredentialsProvider.defaultChain();
    assertNull(creds.credentials()); // null signals "use the SDK default chain"
    creds.close(); // no-op, must not throw
  }
}
