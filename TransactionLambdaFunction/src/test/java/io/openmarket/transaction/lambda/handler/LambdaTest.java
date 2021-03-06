package io.openmarket.transaction.lambda.handler;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import com.amazonaws.services.dynamodbv2.local.shared.access.AmazonDynamoDBLocal;
import com.amazonaws.services.dynamodbv2.model.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.openmarket.transaction.dao.dynamodb.TransactionDaoImpl;
import io.openmarket.transaction.model.Transaction;
import io.openmarket.transaction.model.TransactionErrorType;
import io.openmarket.transaction.model.TransactionStatus;
import io.openmarket.transaction.model.TransactionTaskResult;
import io.openmarket.transaction.model.TransactionType;
import io.openmarket.wallet.dao.dynamodb.WalletDao;
import io.openmarket.wallet.dao.dynamodb.WalletDaoImpl;
import io.openmarket.wallet.model.Wallet;
import io.openmarket.wallet.model.WalletType;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.openmarket.config.TransactionConfig.*;
import static io.openmarket.config.TransactionConfig.TRANSACTION_DDB_ATTRIBUTE_ID;
import static io.openmarket.config.WalletConfig.WALLET_DDB_ATTRIBUTE_OWNER_ID;
import static io.openmarket.config.WalletConfig.WALLET_DDB_TABLE_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class LambdaTest {
    private static final String PAYER_ID = "123";
    private static final String RECIPIENT_ID = "321";
    private static final String CURRENCY_ID = "666";
    private static final String CURRENCY_ID_2 = "777";
    private static final String CURRENCY_ID_3 = "888";
    private static final double TRANSACTION_AMOUNT = 5.00;
    private static final double INITIAL_BALANCE = 100.00;
    private static final Map<String, Double> SINGLE_CURRENCY_WALLET = ImmutableMap.of(CURRENCY_ID, INITIAL_BALANCE);
    private static final Map<String, Double> SINGLE_CURRENCY_WALLET_ZERO_BALANCE = ImmutableMap.of(CURRENCY_ID, 0.0);
    private static final Map<String, Double> MULTIPLE_CURRENCY_WALLET = ImmutableMap.of(CURRENCY_ID, INITIAL_BALANCE,
            CURRENCY_ID_2, INITIAL_BALANCE, CURRENCY_ID_3, INITIAL_BALANCE);
    private static final Map<String, Double> NO_CURRENCY_WALLET = Collections.emptyMap();

    private static AmazonDynamoDBLocal localDBClient;
    private AmazonDynamoDB dbClient;
    private DynamoDBMapper dbMapper;
    private TransactionDaoImpl transactionDao;
    private WalletDao walletDao;
    private TransactionLambda lambda;

    @BeforeAll
    public static void setupLocalDB() {
        localDBClient = DynamoDBEmbedded.create();
    }

    @BeforeEach
    public void setup() {
        dbClient = localDBClient.amazonDynamoDB();
        dbMapper = new DynamoDBMapper(dbClient);
        transactionDao = new TransactionDaoImpl(dbClient, dbMapper);
        walletDao = new WalletDaoImpl(dbClient, dbMapper);
        lambda = new TransactionLambda(transactionDao, walletDao);
        createTable();
    }

    @AfterEach
    public void reset() {
        dbClient.deleteTable(TRANSACTION_DDB_TABLE_NAME);
        dbClient.deleteTable(WALLET_DDB_TABLE_NAME);
    }

    @AfterAll
    public static void tearDown() {
        localDBClient.shutdown();
    }

    @Test
    public void test_Correct_Transaction_Task_Result_Normal() {
        Transaction transaction = createTransaction(TRANSACTION_AMOUNT);

        createUserWallet(PAYER_ID, SINGLE_CURRENCY_WALLET);
        createUserWallet(RECIPIENT_ID, SINGLE_CURRENCY_WALLET);

        TransactionTaskResult result = lambda.processTransaction(transaction);
        assertEquals(transaction.getTransactionId(), result.getTransactionId());
        assertEquals(transaction.getType(), result.getType());
        assertEquals(TransactionErrorType.NONE, result.getError());
        assertEquals(TransactionStatus.COMPLETED, result.getStatus());
    }

    @Test
    public void test_Correct_Transaction_Task_Result_Insufficient_Balance() {
        Transaction transaction = createTransaction(TRANSACTION_AMOUNT);

        createUserWallet(PAYER_ID, SINGLE_CURRENCY_WALLET_ZERO_BALANCE);
        createUserWallet(RECIPIENT_ID, SINGLE_CURRENCY_WALLET);

        TransactionTaskResult result = lambda.processTransaction(transaction);
        assertEquals(transaction.getTransactionId(), result.getTransactionId());
        assertEquals(transaction.getType(), result.getType());
        assertEquals(TransactionErrorType.INSUFFICIENT_BALANCE, result.getError());
        assertEquals(TransactionStatus.ERROR, result.getStatus());
    }

    @Test
    public void test_Add_Currency_To_Wallet() {
        String myCurrency = "6756";
        createUserWallet(RECIPIENT_ID, SINGLE_CURRENCY_WALLET);
        lambda.createCurrencySlot(RECIPIENT_ID, myCurrency);
        Wallet wallet = walletDao.load(RECIPIENT_ID).get();
        assertEquals(2, wallet.getCoins().size());
        assertEquals(0.0, wallet.getCoins().get(myCurrency));
    }

    @Test
    public void check_Transaction_Basic() {
        Transaction transaction = createTransaction(TRANSACTION_AMOUNT);

        createUserWallet(PAYER_ID, SINGLE_CURRENCY_WALLET);
        createUserWallet(RECIPIENT_ID, SINGLE_CURRENCY_WALLET);

        lambda.processTransaction(transaction);

        transaction = transactionDao.load(transaction.getTransactionId()).get();
        assertEquals(TransactionStatus.COMPLETED, transaction.getStatus());
        verify(transaction.getTransactionId(), INITIAL_BALANCE, INITIAL_BALANCE, PAYER_ID, RECIPIENT_ID);
    }

    @Test
    public void check_Transaction_Insufficient_Balance() {
        Transaction transaction = createTransaction(TRANSACTION_AMOUNT);

        createUserWallet(PAYER_ID, SINGLE_CURRENCY_WALLET_ZERO_BALANCE);
        createUserWallet(RECIPIENT_ID, SINGLE_CURRENCY_WALLET);

        lambda.processTransaction(transaction);

        transaction = transactionDao.load(transaction.getTransactionId()).get();
        assertEquals(TransactionStatus.ERROR, transaction.getStatus());
        assertEquals(TransactionErrorType.INSUFFICIENT_BALANCE, transaction.getError());
        verify(transaction.getTransactionId(), 0.0, INITIAL_BALANCE, PAYER_ID, RECIPIENT_ID);
    }

    @Test
    public void check_Transaction_Recipient_No_Such_Coin() {
        Transaction transaction = createTransaction(TRANSACTION_AMOUNT);

        createUserWallet(PAYER_ID, SINGLE_CURRENCY_WALLET);
        createUserWallet(RECIPIENT_ID, NO_CURRENCY_WALLET);

        lambda.processTransaction(transaction);
        verify(transaction.getTransactionId(), INITIAL_BALANCE, 0.0, PAYER_ID, RECIPIENT_ID);
    }

    @Test
    public void check_Transaction_Payer_No_Such_Coin() {
        Transaction transaction = createTransaction(TRANSACTION_AMOUNT);

        createUserWallet(PAYER_ID, NO_CURRENCY_WALLET);
        createUserWallet(RECIPIENT_ID, SINGLE_CURRENCY_WALLET);

        lambda.processTransaction(transaction);
        verify(transaction.getTransactionId(), 0.0, INITIAL_BALANCE, PAYER_ID, RECIPIENT_ID);
    }

    @Test
    public void check_Refund_Basic() {
        Transaction transaction = createTransaction(TRANSACTION_AMOUNT);

        Wallet payer = createUserWallet(PAYER_ID, SINGLE_CURRENCY_WALLET);
        Wallet recipient = createUserWallet(RECIPIENT_ID, SINGLE_CURRENCY_WALLET);

        lambda.processTransaction(transaction);

        Transaction refundTransaction = createRefundTransaction(transaction);
        lambda.processTransaction(refundTransaction);

        verifyRefund(refundTransaction.getTransactionId(), payer, recipient, PAYER_ID, RECIPIENT_ID);
    }

    @Test
    public void test_Racing_Transactions() {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        Transaction transaction1 = createTransaction(INITIAL_BALANCE);
        Transaction transaction2 = createTransaction(INITIAL_BALANCE);
        List<Transaction> trans = ImmutableList.of(transaction1, transaction2);

        createUserWallet(PAYER_ID, SINGLE_CURRENCY_WALLET);
        createUserWallet(RECIPIENT_ID, NO_CURRENCY_WALLET);

        runTask(executor, trans);

        trans = trans.stream().map(a -> transactionDao.load(a.getTransactionId()).get()).collect(Collectors.toList());
        trans = trans.stream().filter(a -> a.getStatus().equals(TransactionStatus.ERROR)).collect(Collectors.toList());
        assertEquals(1, trans.size());
        verifyMultipleTransactions(trans.stream().map(a -> a.getTransactionId()).collect(Collectors.toList()),
                INITIAL_BALANCE, INITIAL_BALANCE, PAYER_ID, RECIPIENT_ID);
    }

    @Test
    public void test_Recipient_Has_Multiple_Coins_Concurrent_Transaction() {
        ExecutorService executor = Executors.newFixedThreadPool(3);
        Transaction transaction1 = createTransaction(CURRENCY_ID, 10.0);
        Transaction transaction2 = createTransaction(CURRENCY_ID_2, 20.0);
        Transaction transaction3 = createTransaction(CURRENCY_ID_3, 30.0);
        List<Transaction> trans = ImmutableList.of(transaction1, transaction2, transaction3);
        createUserWallet(PAYER_ID, MULTIPLE_CURRENCY_WALLET);
        createUserWallet(RECIPIENT_ID, MULTIPLE_CURRENCY_WALLET);
        runTask(executor, trans);
        verifyMultipleTransactions(trans.stream().map(Transaction::getTransactionId).collect(Collectors.toList()),
                INITIAL_BALANCE, INITIAL_BALANCE, PAYER_ID, RECIPIENT_ID);
    }

    @ParameterizedTest
    @MethodSource("getCheckMultipleParams")
    public void check_Multiple_Transactions_Same_Coin(int count, int worker) {
        ExecutorService executor = Executors.newFixedThreadPool(worker);
        createUserWallet(PAYER_ID, SINGLE_CURRENCY_WALLET);
        createUserWallet(RECIPIENT_ID, SINGLE_CURRENCY_WALLET_ZERO_BALANCE);
        Set<Transaction> trans = new HashSet<>();
        for (int i = 0; i < count; i++) {
            Transaction t = createTransaction(TRANSACTION_AMOUNT);
            trans.add(t);
        }
        runTask(executor, trans);

        for (Transaction t : trans) {
            t = transactionDao.load(t.getTransactionId()).get();
            assertEquals(TransactionStatus.COMPLETED, t.getStatus());
        }
        verifyMultipleTransactions(trans.stream().map(Transaction::getTransactionId).collect(Collectors.toSet()),
                INITIAL_BALANCE, 0.0, PAYER_ID, RECIPIENT_ID);
    }

    private static Stream<Arguments> getCheckMultipleParams() {
        return Stream.of(
                Arguments.of(1, 1),
                Arguments.of(2, 1),
                Arguments.of(5, 2),
                Arguments.of(10, 5),
                Arguments.of(20, 10)
        );
    }

    @Test
    public void test_Racing_Refunds() {
        ExecutorService executor = Executors.newFixedThreadPool(5);
        Transaction t = createTransaction(TRANSACTION_AMOUNT);

        Wallet a = createUserWallet(PAYER_ID, SINGLE_CURRENCY_WALLET);
        Wallet b = createUserWallet(RECIPIENT_ID, SINGLE_CURRENCY_WALLET);

        lambda.processTransaction(t);

        List<Transaction> refunds = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            refunds.add(createRefundTransaction(t));
        }

        runTask(executor, refunds);

        refunds = refunds.stream()
                .map(x -> transactionDao.load(x.getTransactionId()).get())
                .filter(k -> k.getStatus().equals(TransactionStatus.COMPLETED)).collect(Collectors.toList());
        assertEquals(1, refunds.size());
        verifyRefund(refunds.get(0).getTransactionId(), a, b, PAYER_ID, RECIPIENT_ID);
    }

    @Test
    public void test_Cannot_Refund_If_Insufficient_Balance() {
        Transaction t = createTransaction(TRANSACTION_AMOUNT);

        createUserWallet(PAYER_ID, SINGLE_CURRENCY_WALLET);
        createUserWallet(RECIPIENT_ID, SINGLE_CURRENCY_WALLET);

        lambda.processTransaction(t);

        Wallet b = walletDao.load(RECIPIENT_ID).get();
        b.setCoins(SINGLE_CURRENCY_WALLET_ZERO_BALANCE);
        walletDao.save(b);

        Transaction refundTransaction = createRefundTransaction(t);
        lambda.processTransaction(refundTransaction);

        refundTransaction = transactionDao.load(refundTransaction.getTransactionId()).get();
        t = transactionDao.load(t.getTransactionId()).get();
        assertEquals(TransactionErrorType.INSUFFICIENT_BALANCE, refundTransaction.getError());
        assertEquals(TransactionStatus.COMPLETED, t.getStatus());
    }

    @Test
    public void test_Refund_Failure_If_Not_Refund_Started() {
        Transaction t = createTransaction(TRANSACTION_AMOUNT);

        createUserWallet(PAYER_ID, SINGLE_CURRENCY_WALLET);
        createUserWallet(RECIPIENT_ID, SINGLE_CURRENCY_WALLET);

        lambda.processTransaction(t);

        Transaction refund = createRefundTransaction(t);

        // Modify the transaction after a refund has been created should fail the refund
        t = transactionDao.load(t.getTransactionId()).get();
        t.setStatus(TransactionStatus.COMPLETED);
        transactionDao.save(t);

        lambda.processTransaction(refund);

        // Final effect should be the transaction, not the refund
        verify(t.getTransactionId(), INITIAL_BALANCE, INITIAL_BALANCE, PAYER_ID, RECIPIENT_ID);
    }

    @Test
    public void test_Refund_Multiple_Currencies() {
        ExecutorService executor = Executors.newFixedThreadPool(5);
        Transaction t1 = createTransaction(CURRENCY_ID, TRANSACTION_AMOUNT);
        Transaction t2 = createTransaction(CURRENCY_ID_2, TRANSACTION_AMOUNT);
        Transaction t3 = createTransaction(CURRENCY_ID_3, TRANSACTION_AMOUNT);

        createUserWallet(PAYER_ID, MULTIPLE_CURRENCY_WALLET);
        createUserWallet(RECIPIENT_ID, MULTIPLE_CURRENCY_WALLET);

        lambda.processTransaction(t1);
        lambda.processTransaction(t2);
        lambda.processTransaction(t3);

        List<Transaction> refunds = ImmutableList.of(createRefundTransaction(t1), createRefundTransaction(t2),
                createRefundTransaction(t3));
        runTask(executor, refunds);

        assertEquals(MULTIPLE_CURRENCY_WALLET, walletDao.load(PAYER_ID).get().getCoins());
        assertEquals(MULTIPLE_CURRENCY_WALLET, walletDao.load(RECIPIENT_ID).get().getCoins());
    }

    @Test
    public void test_No_Such_Recipient() {
        Transaction t = createTransaction(TRANSACTION_AMOUNT);
        createUserWallet(PAYER_ID, SINGLE_CURRENCY_WALLET);

        lambda.processTransaction(t);

        t = transactionDao.load(t.getTransactionId()).get();
        assertEquals(TransactionStatus.ERROR, t.getStatus());
    }

    private void runTask(ExecutorService executorService, Collection<Transaction> transactions) {
        for (Transaction r : transactions) {
            executorService.submit(() -> lambda.processTransaction(r));
        }
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(2, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
        }
    }

    private void verifyMultipleTransactions(Collection<String> transactions,
                                            Double payerBeforeBalance, Double recipientBeforeBalance,
                                            String payerId, String recipientId) {
        Map<String, Double> amount = new HashMap<>();
        for (String t : transactions) {
            Transaction transaction = transactionDao.load(t).get();
            if (transaction.getStatus().equals(TransactionStatus.COMPLETED)) {
                if (!amount.containsKey(transaction.getCurrencyId())) {
                    amount.put(transaction.getCurrencyId(), 0.0);
                }
                amount.put(transaction.getCurrencyId(),
                        amount.get(transaction.getCurrencyId()) + transaction.getAmount());
            }
        }
        Map<String, Double> payerAfterBalance = walletDao.load(payerId).get().getCoins();
        Map<String, Double> recipientAfterBalance = walletDao.load(recipientId).get().getCoins();

        for (String currency : amount.keySet()) {
            Double expectedPayerBalance = payerBeforeBalance - amount.get(currency);
            Double expectedPRecipientBalance = recipientBeforeBalance + amount.get(currency);
            assertEquals(expectedPayerBalance, payerAfterBalance.get(currency));
            assertEquals(expectedPRecipientBalance, recipientAfterBalance.get(currency));
        }
    }

    private void verify(String transactionId, Double payerBeforeBalance, Double recipientBeforeBalance,
                        String payerId, String recipientId) {
        Transaction transaction = transactionDao.load(transactionId).get();
        Double payerAfterBalance = walletDao.load(payerId).get().getCoins()
                .getOrDefault(transaction.getCurrencyId(), 0.0);
        Double recipientAfterBalance = walletDao.load(recipientId).get().getCoins()
                .getOrDefault(transaction.getCurrencyId(), 0.0);
        if (transaction.getStatus().equals(TransactionStatus.COMPLETED)) {
            Double truePayerAfterBalance = payerBeforeBalance - transaction.getAmount();
            Double trueRecipientAfterBalance = recipientBeforeBalance + transaction.getAmount();
            assertEquals(truePayerAfterBalance, payerAfterBalance);
            assertEquals(trueRecipientAfterBalance, recipientAfterBalance);
        } else {
            assertEquals(payerBeforeBalance, payerAfterBalance);
            assertEquals(recipientBeforeBalance, recipientAfterBalance);
        }
    }

    private void verifyRefund(String refundTransactionId, Wallet payerBeforeWallet, Wallet recipientBeforeWallet,
                                       String payerId, String recipientId) {
        Transaction transaction = transactionDao.load(refundTransactionId).get();
        assertEquals(TransactionStatus.COMPLETED, transaction.getStatus());

        Transaction orgTransaction = transactionDao.load(transaction.getRefundTransacIds().get(0)).get();
        assertEquals(TransactionStatus.REFUNDED, orgTransaction.getStatus());

        Wallet payerAfterWallet = walletDao.load(payerId).get();
        Wallet recipientAfterWallet = walletDao.load(recipientId).get();
        assertEquals(payerBeforeWallet, payerAfterWallet);
        assertEquals(recipientBeforeWallet, recipientAfterWallet);
    }

    private Wallet createUserWallet(String ownerId, Map<String, Double> coins) {
        Wallet wallet = Wallet.builder().ownerId(ownerId).coins(coins).type(WalletType.USER).build();
        walletDao.save(wallet);
        return wallet;
    }

    private Transaction createTransaction(double amount) {
        return createTransaction(CURRENCY_ID, amount);
    }

    private Transaction createTransaction(String currencyId, double amount) {
        Transaction transaction = Transaction.builder()
                .transactionId(UUID.randomUUID().toString())
                .currencyId(currencyId)
                .amount(amount)
                .payerId(PAYER_ID).recipientId(RECIPIENT_ID)
                .status(TransactionStatus.PENDING)
                .type(TransactionType.TRANSFER)
                .build();
        transactionDao.save(transaction);
        return transaction;
    }

    private Transaction createRefundTransaction(Transaction transaction) {
        Transaction refundTransaction = Transaction.builder()
                .transactionId(UUID.randomUUID().toString())
                .currencyId(transaction.getCurrencyId())
                .amount(transaction.getAmount())
                .payerId(transaction.getRecipientId())
                .recipientId(transaction.getPayerId())
                .status(TransactionStatus.PENDING)
                .type(TransactionType.REFUND)
                .refundTransacIds(ImmutableList.of(transaction.getTransactionId()))
                .build();
        transactionDao.save(refundTransaction);

        transaction = transactionDao.load(transaction.getTransactionId()).get();
        transaction.setStatus(TransactionStatus.REFUND_STARTED);
        transactionDao.save(transaction);
        return refundTransaction;
    }

    private static void createTable() {
        ProvisionedThroughput throughput = new ProvisionedThroughput(5L, 5L);
        localDBClient.amazonDynamoDB().createTable(new CreateTableRequest().withTableName(TRANSACTION_DDB_TABLE_NAME)
                .withKeySchema(ImmutableList.of(new KeySchemaElement(TRANSACTION_DDB_ATTRIBUTE_ID, KeyType.HASH)))
                .withAttributeDefinitions(new AttributeDefinition(TRANSACTION_DDB_ATTRIBUTE_ID, ScalarAttributeType.S))
                .withProvisionedThroughput(throughput));

        localDBClient.amazonDynamoDB().createTable(new CreateTableRequest().withTableName(WALLET_DDB_TABLE_NAME)
                .withKeySchema(ImmutableList.of(new KeySchemaElement(WALLET_DDB_ATTRIBUTE_OWNER_ID, KeyType.HASH)))
                .withAttributeDefinitions(
                        new AttributeDefinition(WALLET_DDB_ATTRIBUTE_OWNER_ID, ScalarAttributeType.S))
                .withProvisionedThroughput(throughput));
    }
}
