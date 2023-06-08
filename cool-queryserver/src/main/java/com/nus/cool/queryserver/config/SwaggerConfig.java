package com.nus.cool.queryserver.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import springfox.documentation.builders.ApiInfoBuilder;
import springfox.documentation.builders.PathSelectors;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.service.ApiInfo;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

/**
 * SwaggerConfig.
 */
@Configuration // Mark this class as a configuration class
@EnableSwagger2 // Enable Swagger 2 for the application
public class SwaggerConfig {

  /**
   * This is the main page of Swagger.
   */
  @Bean // Declare a Spring bean for the Swagger Docket configuration
  public Docket createRestApi() {
    return new Docket(DocumentationType.SWAGGER_2).apiInfo(apiInfo()).select().apis(
            RequestHandlerSelectors.basePackage(
                "com.nus.cool.queryserver.handler")) // Scan the specified package for controllers
        .paths(PathSelectors.any()) // Include all paths for the selected controllers
        .build();
  }

  /**
   * apiInfo.
   */
  private ApiInfo apiInfo() {
    return new ApiInfoBuilder().title("COOL-cohort") // Set the API title
        .description(
            "COOL is a cohort OLAP system specialized for cohort analysis with extremely low "
                + "latency., please only uses cohort-controller") // Set the API description
        .version("1.0") // Set the API version
        .build();
  }
}
