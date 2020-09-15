package io.openmarket.transaction.lambda;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.google.common.collect.ImmutableMap;
import io.openmarket.transaction.dao.dynamodb.TransactionDao;
import io.openmarket.transaction.dao.dynamodb.TransactionDaoImpl;
import io.openmarket.transaction.dao.sqs.SQSTransactionTaskPublisher;
import io.openmarket.transaction.lambda.handler.TransactionLambda;
import io.openmarket.transaction.model.Transaction;
import io.openmarket.transaction.model.TransactionStatus;
import io.openmarket.transaction.model.TransactionTask;
import io.openmarket.transaction.model.TransactionType;
import io.openmarket.wallet.dao.dynamodb.WalletDao;
import io.openmarket.wallet.dao.dynamodb.WalletDaoImpl;
import io.openmarket.wallet.model.Wallet;

import java.util.UUID;

public class CLI {
    public static void main(String[] args) {
        AmazonDynamoDB dbClient = AmazonDynamoDBClientBuilder.standard().build();
        DynamoDBMapper mapper = new DynamoDBMapper(dbClient);
        TransactionDao dao = new TransactionDaoImpl(dbClient, mapper);
        WalletDao wDao = new WalletDaoImpl(dbClient, mapper);

//        Wallet a = Wallet.builder().ownerId("666").coins(ImmutableMap.of("777", 100.0)).build();
//        Wallet b = Wallet.builder().ownerId("321").coins(ImmutableMap.of("666", 100.0)).build();

//        wDao.save(a);
//        wDao.save(b);

        String transacId = UUID.randomUUID().toString();
        Transaction transaction = Transaction.builder().transactionId(transacId).payerId("456").recipientId("666").status(TransactionStatus.PENDING).amount(60.0).currencyId("666").type(TransactionType.TRANSFER).build();
        dao.save(transaction);

        SQSTransactionTaskPublisher pub = new SQSTransactionTaskPublisher(AmazonSQSClientBuilder.standard().build());
        pub.publish("https://sqs.us-west-2.amazonaws.com/185046651126/TransactionTaskQueue", new TransactionTask(transacId));
//
//        TransactionLambda lambda = new TransactionLambda(dao, wDao);
//        lambda.processTransaction(transaction);
    }
}
