package org.helei.RDBFileResolver.disruptor;

import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import lombok.Data;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Data
public class RedisKVDisruptorClient {

    private RedisKVProducer eventProducer;

    private RingBuffer<RedisKVEvent> disruptorMsgBuffer;

    private Integer writeCount;

    private ExecutorService writeExecutor;

    private ExecutorService dispatchExecutor;

    RedisKVDisruptorClient(Integer writeCount) {

        this.writeCount = Math.max(writeCount, 1);

        dispatchExecutor = Executors.newSingleThreadExecutor();

        writeExecutor = Executors.newFixedThreadPool(writeCount);

        disruptorMsgBuffer = RingBuffer.createSingleProducer(
                // 定义线程池工厂
                new MessageEventFactory(),
                // RingBuffer大小
                1024,
                // 生产者和消费者策略
                new BlockingWaitStrategy());

        eventProducer = new RedisKVProducer(disruptorMsgBuffer);

        // 创建异常处理
        ExceptionHandler<RedisKVEvent> exceptionHandler = new SimpleFatalExceptionHandler();

        // 创建屏障
        SequenceBarrier sequenceBarrier = disruptorMsgBuffer.newBarrier();
        // 定义事件处理器
        BatchEventProcessor<RedisKVEvent> dispatcherStage = new BatchEventProcessor<RedisKVEvent>(
                // RingBuffer
                disruptorMsgBuffer,
                // SequenceBarrier
                sequenceBarrier,
                // EventHandler
                new RedisKVDispatcherHandler());

        // 创建屏障
        SequenceBarrier writeHandlerSequenceBarrier = disruptorMsgBuffer
                .newBarrier(dispatcherStage.getSequence());

        // 创建多个消费者组(WorkHandler)
        WorkHandler<RedisKVEvent>[] workHandlers = new RedisKVWriteHandler[writeCount];
        for (int i = 0; i < writeCount; i++) {
            workHandlers[i] = new RedisKVWriteHandler();
        }

        // 创建WorkPool
        WorkerPool<RedisKVEvent> workerPool = new WorkerPool<RedisKVEvent>(
                // DataProvider
                disruptorMsgBuffer,
                // SequenceBarrier
                writeHandlerSequenceBarrier,
                // 异常处理
                exceptionHandler,
                // WorkHandler数组
                workHandlers);

        // 从WorkPool里获得所有的:Sequence
        Sequence[] sequence = workerPool.getWorkerSequences();
        // 添加:Sequence与RingBuffer的关系,因为:Sequence需要实时向:RingBuffer实时汇报处理情况.
        disruptorMsgBuffer.addGatingSequences(sequence);


        // 定义:SimpleParserStage和SinkStoreStage(EventHandler)交给哪个线程处理
        dispatchExecutor.submit(dispatcherStage);

        // 定义:DmlParserStage(WorkHandler)交给哪个线程池处理.
        workerPool.start(writeExecutor);
    }

    public void shutdown(){

    }
}

/**
 * 业务模型工厂.Disruptor在构建时,会创建一个环形队列,队列里的业务模型就是通过该工厂创建的
 *
 * @author helei
 *
 */
class MessageEventFactory implements EventFactory<RedisKVEvent> {
    @Override
    public RedisKVEvent newInstance() {
        return new RedisKVEvent();
    }
}

/**
 * 异常处理
 *
 * @author helei
 *
 */
class SimpleFatalExceptionHandler implements ExceptionHandler<RedisKVEvent> {
    @Override
    public void handleEventException(Throwable ex, long sequence, RedisKVEvent event) {
    }

    @Override
    public void handleOnStartException(Throwable ex) {
    }

    @Override
    public void handleOnShutdownException(Throwable ex) {

    }
}

