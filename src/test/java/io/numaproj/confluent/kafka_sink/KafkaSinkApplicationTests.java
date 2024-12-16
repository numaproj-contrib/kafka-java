package io.numaproj.confluent.kafka_sink;

import io.numaproj.numaflow.sinker.Server;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import static org.mockito.Mockito.*;

@Slf4j
@ExtendWith(MockitoExtension.class)
class KafkaSinkApplicationTests {

    @Mock
    private ConfigurableApplicationContext configurableApplicationContextMock;

    @Test
    void main_initializeSuccess() throws Exception {
        Server kafkaSinkerServerMock = mock(Server.class);
        doReturn(kafkaSinkerServerMock).when(configurableApplicationContextMock).getBean(Server.class);
        try (MockedConstruction<SpringApplicationBuilder> ignored = Mockito
                .mockConstructionWithAnswer(SpringApplicationBuilder.class,
                        invoke -> configurableApplicationContextMock)
        ) {
            KafkaSinkApplication.main(new String[]{"arg1", "arg2"});
            Mockito.verify(kafkaSinkerServerMock, times(1)).start();
        }
    }
}