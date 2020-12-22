package io.openmarket.transaction.lambda.handler;

import com.amazonaws.services.dynamodbv2.datamodeling.TransactionWriteRequest;
import com.amazonaws.services.dynamodbv2.model.*;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import io.openmarket.transaction.lambda.config.LambdaConfig;
import io.openmarket.transaction.dao.dynamodb.TransactionDao;
import io.openmarket.transaction.model.Transaction;
import io.openmarket.transaction.model.TransactionErrorType;
import io.openmarket.transaction.model.TransactionStatus;
import io.openmarket.transaction.model.TransactionTaskResult;
import io.openmarket.transaction.model.TransactionType;
import io.openmarket.wallet.dao.dynamodb.WalletDao;
import lombok.NonNull;
import lombok.extern.log4j.Log4j2;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.openmarket.config.TransactionConfig.*;
import static io.openmarket.config.WalletConfig.*;

@Log4j2
public class TransactionLambda {
    private static final String ATTR_NAME_COIN_MAP = "#cm";
    private static final String ATTR_NAME_COIN = "#coin";
    private static final String ATTR_NAME_COIN_IN_MAP = String.format("%s.%s", ATTR_NAME_COIN_MAP, ATTR_NAME_COIN);
    private static final String ATTR_VAL_TRANSACTION_AMOUNT = ":val";
    private static final String EXPRESSION_UPDATE_PAYER_BALANCE = String.format("SET %s = %s - %s",
            ATTR_NAME_COIN_IN_MAP, ATTR_NAME_COIN_IN_MAP, ATTR_VAL_TRANSACTION_AMOUNT);
    private static final String EXPRESSION_CHECK_PAYER_ENOUGH_BALANCE = String.format(
            "attribute_exists(%s) AND %s >= %s", ATTR_NAME_COIN_IN_MAP, ATTR_NAME_COIN_IN_MAP,
            ATTR_VAL_TRANSACTION_AMOUNT);
    private static final String EXPRESSION_UPDATE_RECIPIENT_BALANCE = String.format("SET %s = %s + %s",
            ATTR_NAME_COIN_IN_MAP, ATTR_NAME_COIN_IN_MAP, ATTR_VAL_TRANSACTION_AMOUNT);

    private static final String ATTR_NAME_TRANSAC_STATUS = "#stat";
    private static final String ATTR_VAL_TRANSAC_STATUS = ":statVal";
    private static final String ATTR_VAL_COND_TRANSAC_STATUS = ":condVal";
    private static final Map<String, String> TRANSAC_STATUS_ATTR_NAME = ImmutableMap.of(ATTR_NAME_TRANSAC_STATUS,
            TRANSACTION_DDB_ATTRIBUTE_STATUS);
    private static final String EXPRESSION_CHECK_STATUS = String.format("%s = %s", ATTR_NAME_TRANSAC_STATUS,
            ATTR_VAL_COND_TRANSAC_STATUS);


    private static final String EXPRESSION_UPDATE_TRANSAC_STATUS = String.format("SET %s = %s",
            ATTR_NAME_TRANSAC_STATUS, ATTR_VAL_TRANSAC_STATUS);
    private static final String ATTR_VAL_DEFAULT_COIN_AMOUNT = ":default";
    private static final String CREATE_COIN_SLOT_EXPRESSION = String.format("SET %s = %s",
            ATTR_NAME_COIN_IN_MAP, ATTR_VAL_DEFAULT_COIN_AMOUNT);
    private static final String COIN_NOT_ALREADY_EXIST = String.format("attribute_not_exists(%s)",
            ATTR_NAME_COIN_IN_MAP);

    private final TransactionDao transactionDao;
    private final WalletDao walletDao;

    @Inject
    public TransactionLambda(@NonNull final TransactionDao dbDao, @NonNull final WalletDao walletDao) {
        this.transactionDao = dbDao;
        this.walletDao = walletDao;
    }

    public TransactionTaskResult processTransaction(@NonNull final Transaction transaction) {
        final TransactionTaskResult result = TransactionTaskResult.builder()
                .transactionId(transaction.getTransactionId())
                .type(transaction.getType())
                .error(TransactionErrorType.NONE)
                .status(TransactionStatus.COMPLETED)
                .build();
        try {
            log.info("Processing transaction {}", transaction);
            processTransactionHelper(transaction);
        } catch (Exception e) {
            log.error("An exception occurred while processing transaction: {}", transaction, e);
            result.setError(TransactionErrorType.INSUFFICIENT_BALANCE);
            result.setStatus(TransactionStatus.ERROR);
            try {
                updateErrorStatus(transaction, TransactionErrorType.INSUFFICIENT_BALANCE);
            } catch (ConditionalCheckFailedException e2) {
                log.warn("Transaction {} was overwritten externally", transaction.getTransactionId(), e2);
            }
        }
        return result;
    }

    @VisibleForTesting
    protected void processTransactionHelper(final Transaction transaction) {
        final Map<String, AttributeValue> payerKey = getOwnerKey(transaction.getPayerId());
        final Map<String, AttributeValue> recipientKey = getOwnerKey(transaction.getRecipientId());
        final Map<String, String> attributeNames = getAttributeName(transaction.getCurrencyId());
        final Map<String, AttributeValue> attributeValues = getAttributeValue(transaction.getAmount());

        // Create the coin slot if it doesn't already exist.
        createCurrencySlot(transaction.getRecipientId(), transaction.getCurrencyId());

        final List<TransactWriteItem> updateRequests = Stream.of(
                new TransactWriteItem().withUpdate(new Update()
                        .withKey(payerKey)
                        .withUpdateExpression(EXPRESSION_UPDATE_PAYER_BALANCE)
                        .withConditionExpression(EXPRESSION_CHECK_PAYER_ENOUGH_BALANCE)
                        .withExpressionAttributeNames(attributeNames)
                        .withExpressionAttributeValues(attributeValues)
                        .withTableName(WALLET_DDB_TABLE_NAME)
                ),
                new TransactWriteItem().withUpdate(new Update()
                        .withKey(recipientKey)
                        .withUpdateExpression(EXPRESSION_UPDATE_RECIPIENT_BALANCE)
                        .withExpressionAttributeNames(attributeNames)
                        .withExpressionAttributeValues(attributeValues)
                        .withTableName(WALLET_DDB_TABLE_NAME)
                ),
                new TransactWriteItem().withUpdate(new Update()
                        .withKey(getTransacKey(transaction.getTransactionId()))
                        .withUpdateExpression(EXPRESSION_UPDATE_TRANSAC_STATUS)
                        .withConditionExpression(EXPRESSION_CHECK_STATUS)
                        .withExpressionAttributeNames(TRANSAC_STATUS_ATTR_NAME)
                        .withExpressionAttributeValues(getTransacValue(TransactionStatus.COMPLETED,
                                TransactionStatus.PENDING))

                        .withTableName(TRANSACTION_DDB_TABLE_NAME)
                )
        ).collect(Collectors.toList());
        if (transaction.getType().equals(TransactionType.REFUND)) {
            updateRequests.add(new TransactWriteItem()
                    .withUpdate(new Update()
                    .withKey(getTransacKey(transaction.getRefundTransacIds().get(0)))
                    .withUpdateExpression(EXPRESSION_UPDATE_TRANSAC_STATUS)
                    .withConditionExpression(EXPRESSION_CHECK_STATUS)
                    .withExpressionAttributeNames(TRANSAC_STATUS_ATTR_NAME)
                    .withExpressionAttributeValues(getTransacValue(TransactionStatus.REFUNDED,
                            TransactionStatus.REFUND_STARTED))
                    .withTableName(TRANSACTION_DDB_TABLE_NAME)));
        }
        walletDao.doTransactionWrite(updateRequests);
    }

    @VisibleForTesting
    protected void createCurrencySlot(final String ownerId, String currencyId) {
        final Map<String, AttributeValue> key = getOwnerKey(ownerId);
        final UpdateItemRequest request = new UpdateItemRequest()
                .withTableName(WALLET_DDB_TABLE_NAME)
                .withKey(key)
                .withUpdateExpression(CREATE_COIN_SLOT_EXPRESSION)
                .withConditionExpression(COIN_NOT_ALREADY_EXIST)
                .withExpressionAttributeNames(getAttributeName(currencyId))
                .withExpressionAttributeValues(getCurrencySlotAttrValue());
        try {
            walletDao.update(request);
            log.info("Created a new coin for ownerId '{}'", ownerId);
        } catch (ConditionalCheckFailedException e) {
            log.info("Owner '{}' already have currency with Id {}", ownerId, currencyId);
        }
    }

    private void updateErrorStatus(final Transaction transaction, final TransactionErrorType error) {
        transaction.setStatus(TransactionStatus.ERROR);
        transaction.setError(error);
        if (transaction.getType().equals(TransactionType.REFUND)) {
            final Transaction orgTransaction = transactionDao.load(transaction.getRefundTransacIds().get(0)).get();
            if (orgTransaction.getStatus().equals(TransactionStatus.REFUND_STARTED)) {
                orgTransaction.setStatus(TransactionStatus.COMPLETED);
                transactionDao.transactionWrite(new TransactionWriteRequest()
                        .addUpdate(transaction)
                        .addUpdate(orgTransaction));
                return;
            }
            log.warn("Refund for transaction '{}' has invalid status {}, status {} is not auto updated",
                    orgTransaction.getTransactionId(), orgTransaction.getStatus(), TransactionStatus.COMPLETED);
        }
        transactionDao.save(transaction);
    }

    private static Map<String, AttributeValue> getCurrencySlotAttrValue() {
        return ImmutableMap.of(ATTR_VAL_DEFAULT_COIN_AMOUNT, new AttributeValue()
                .withN(String.valueOf(LambdaConfig.INITIAL_COIN_AMOUNT)));
    }

    private static Map<String, AttributeValue> getTransacKey(final String transactionId) {
        return ImmutableMap.of(TRANSACTION_DDB_ATTRIBUTE_ID, new AttributeValue(transactionId));
    }

    private static Map<String, AttributeValue> getTransacValue(final TransactionStatus finalstatus,
                                                               final TransactionStatus preStatus) {
        return ImmutableMap.of(ATTR_VAL_TRANSAC_STATUS, new AttributeValue(String.valueOf(finalstatus)),
                ATTR_VAL_COND_TRANSAC_STATUS, new AttributeValue(preStatus.toString()));
    }

    private static Map<String, AttributeValue> getOwnerKey(final String payerId) {
        return ImmutableMap.of(WALLET_DDB_ATTRIBUTE_OWNER_ID,
                new AttributeValue(payerId));
    }

    private static Map<String, String> getAttributeName(final String currencyId) {
        return ImmutableMap.of(ATTR_NAME_COIN_MAP, WALLET_DDB_ATTRIBUTE_COIN_MAP,
                ATTR_NAME_COIN, currencyId);
    }

    private static  Map<String, AttributeValue> getAttributeValue(final double amount) {
        return ImmutableMap.of(
                ATTR_VAL_TRANSACTION_AMOUNT, new AttributeValue().withN(String.valueOf(amount)));
    }
}
