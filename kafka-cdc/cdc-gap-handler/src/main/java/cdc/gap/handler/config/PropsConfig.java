package cdc.gap.handler.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(GapHandlerProps.class)
public class PropsConfig {}

