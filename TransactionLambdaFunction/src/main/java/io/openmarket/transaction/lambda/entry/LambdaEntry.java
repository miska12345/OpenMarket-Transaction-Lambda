package io.openmarket.transaction.lambda.entry;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.google.gson.Gson;
import io.openmarket.sns.dao.SNSDao;
import io.openmarket.sns.dao.SNSDaoImpl;
import io.openmarket.transaction.dao.dynamodb.TransactionDao;
import io.openmarket.transaction.dao.dynamodb.TransactionDaoImpl;
import io.openmarket.transaction.lambda.handler.TransactionLambda;
import io.openmarket.transaction.model.Transaction;
import io.openmarket.transaction.model.TransactionTask;
import io.openmarket.transaction.model.TransactionTaskResult;
import io.openmarket.wallet.dao.dynamodb.WalletDao;
import io.openmarket.wallet.dao.dynamodb.WalletDaoImpl;
import lombok.extern.log4j.Log4j2;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import static io.openmarket.transaction.lambda.config.EnvironmentConfig.ENV_VAR_ENABLE_PUBLISH_TO_TOPIC;
import static io.openmarket.transaction.lambda.config.EnvironmentConfig.ENV_VAR_ON_PROCESSED_TOPIC_ARN;

@Log4j2
public class LambdaEntry implements RequestHandler<SQSEvent, List<TransactionTaskResult>> {
    private static final Gson GSON = new Gson();
    public List<TransactionTaskResult> handleRequest(final SQSEvent input, final Context context) {
        log.info("Lambda is handling transaction requests, size: {}", input.getRecords().size());
        final AmazonDynamoDB dbClient = AmazonDynamoDBClientBuilder.standard().build();
        final DynamoDBMapper mapper = new DynamoDBMapper(dbClient);
        final TransactionDao transacDao = new TransactionDaoImpl(dbClient, mapper);
        final WalletDao walletDao = new WalletDaoImpl(dbClient, mapper);
        final TransactionLambda lambda = new TransactionLambda(transacDao, walletDao);
        final SNSDao snsDao = new SNSDaoImpl(AmazonSNSClientBuilder.standard().build(), GSON);
        final boolean publishToSNS = Boolean.parseBoolean(System.getenv(ENV_VAR_ENABLE_PUBLISH_TO_TOPIC));
        final String snsTopicARN = System.getenv(ENV_VAR_ON_PROCESSED_TOPIC_ARN);

        final List<TransactionTask> tasks = input.getRecords()
                .stream().map(a -> GSON.fromJson(a.getBody(), TransactionTask.class))
                .collect(Collectors.toList());
        final List<Transaction> transactions = transacDao.batchLoad(tasks.stream()
                .map(TransactionTask::getTransactionId).collect(Collectors.toList()));

        final List<TransactionTaskResult> results = new LinkedList<>();
        for (Transaction t : transactions) {
            results.add(lambda.processTransaction(t));
        }

        if (publishToSNS) {
            try {
                snsDao.publishToTopic(snsTopicARN, GSON.toJson(results));
                log.info("Published to SNS topicARN: {}", snsTopicARN);
            } catch (IllegalArgumentException e) {
                log.error("Failed to publish to SNS topicArn: {}", snsTopicARN, e);
            }
        }
        log.info("Finished processing {} transactions", results.size());
        return results;
    }
}
