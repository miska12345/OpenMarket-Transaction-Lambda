package io.openmarket.transaction.lambda.entry;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.google.gson.Gson;
import io.openmarket.transaction.dao.dynamodb.TransactionDao;
import io.openmarket.transaction.dao.dynamodb.TransactionDaoImpl;
import io.openmarket.transaction.lambda.handler.TransactionLambda;
import io.openmarket.transaction.model.Transaction;
import io.openmarket.transaction.model.TransactionTask;
import io.openmarket.wallet.dao.dynamodb.WalletDao;
import io.openmarket.wallet.dao.dynamodb.WalletDaoImpl;
import lombok.extern.log4j.Log4j2;

import java.util.List;
import java.util.stream.Collectors;

@Log4j2
public class LambdaEntry implements RequestHandler<SQSEvent, Void> {
    private static final Gson GSON = new Gson();
    public Void handleRequest(final SQSEvent input, final Context context) {
        log.info("Lambda is handling transaction request, size: {}", input.getRecords().size());
        final AmazonDynamoDB dbClient = AmazonDynamoDBClientBuilder.standard().build();
        final DynamoDBMapper mapper = new DynamoDBMapper(dbClient);
        final TransactionDao transacDao = new TransactionDaoImpl(dbClient, mapper);
        final WalletDao walletDao = new WalletDaoImpl(dbClient, mapper);
        final TransactionLambda lambda = new TransactionLambda(transacDao, walletDao);

        final List<TransactionTask> tasks = input.getRecords()
                .stream().map(a -> GSON.fromJson(a.getBody(), TransactionTask.class))
                .collect(Collectors.toList());
        final List<Transaction> transactions = transacDao.batchLoad(tasks.stream()
                .map(TransactionTask::getTransactionId).collect(Collectors.toList()));
        transactions.forEach(lambda::processTransaction);
        return null;
    }
}
