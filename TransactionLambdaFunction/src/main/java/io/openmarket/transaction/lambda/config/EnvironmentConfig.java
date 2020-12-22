package io.openmarket.transaction.lambda.config;

public class EnvironmentConfig {
    /**
     * The environmental variable name for whether SNS message should be published.
     */
    public static final String ENV_VAR_ENABLE_PUBLISH_TO_TOPIC = "enablePublishToSNS";

    /**
     * The environmental variable name for the SNS topic ARN on processed transactions.
     */
    public static final String ENV_VAR_ON_PROCESSED_TOPIC_ARN = "onProcessedTopicArn";
}
