package com.alibaba.otter.canal.store.memory.buffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.alibaba.otter.canal.common.utils.NamedThreadFactory;
import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.WorkHandler;
import com.lmax.disruptor.WorkerPool;

public class RingBufferTest {
    public static void main(String[] args) throws Exception {
        // 定义实例
        String destination = "example";

        // 创建 RingBuffer
        RingBuffer<MessageEvent> ringBuffer = RingBuffer.createSingleProducer(new MessageEventFactory(), 1024, new BlockingWaitStrategy());

        // 1.开启线程执行 simple parser state 任务
        ExecutorService simpleParseExecutor = Executors.newFixedThreadPool(1, new NamedThreadFactory("simpleParseExecutor-" + destination));
        SequenceBarrier sequenceBarrier = ringBuffer.newBarrier();
        BatchEventProcessor<MessageEvent> simpleParserStage = new BatchEventProcessor<>(ringBuffer, sequenceBarrier, new SimpleParserStage());
        simpleParseExecutor.submit(simpleParserStage);

        // 2.开启 dml parse 任务
        int workCount = 6;
        WorkHandler<MessageEvent>[] workHandlers = new DmlParserStage[workCount];
        for (int i = 0; i < workCount; i++) {
            workHandlers[i] = new DmlParserStage();
        }
        ExceptionHandler<MessageEvent> exceptionHandler = new SimpleFatalExceptionHandler();
        // 基于 simple parser state 阶段的 sequenceBarrier 屏障
        SequenceBarrier dmlParserSequenceBarrier = ringBuffer.newBarrier(simpleParserStage.getSequence());
        WorkerPool<MessageEvent> workerPool = new WorkerPool<>(ringBuffer, dmlParserSequenceBarrier, exceptionHandler, workHandlers);
        Sequence[] sequence = workerPool.getWorkerSequences();
        ringBuffer.addGatingSequences(sequence);
        ExecutorService parserExecutor = Executors.newFixedThreadPool(workCount, new NamedThreadFactory("parserExecutor-" + destination));
        workerPool.start(parserExecutor);

        // 3.开启线程执行 sink store 任务
        // 基于 dml parse 阶段的 sequence 屏障
        SequenceBarrier sinkSequenceBarrier = ringBuffer.newBarrier(sequence);
        BatchEventProcessor<MessageEvent> sinkStoreStage = new BatchEventProcessor<>(ringBuffer, sinkSequenceBarrier, new SinkStoreStage());
        sinkStoreStage.setExceptionHandler(exceptionHandler);
        ringBuffer.addGatingSequences(sinkStoreStage.getSequence());
        ExecutorService sinkParseExecutor = Executors.newFixedThreadPool(1, new NamedThreadFactory("sinkParseExecutor-" + destination));
        sinkParseExecutor.submit(sinkStoreStage);

        // 4.生产
        MessageEventProducer producer = new MessageEventProducer(ringBuffer);
        CountDownLatch latch = new CountDownLatch(1);
        long count = 100;
        for (long i = 1; i <= count; i++) {
            producer.publish(i);
        }
        latch.await();
    }
}

/**
 * 自定义生产者
 */
class MessageEventProducer {
    private final RingBuffer<MessageEvent> ringBuffer;

    public MessageEventProducer(RingBuffer<MessageEvent> ringBuffer) {
        this.ringBuffer = ringBuffer;
    }

    public void publish(Long value) {
        long sequence = ringBuffer.next();
        try {
            MessageEvent event = ringBuffer.get(sequence);
            event.setValue(value);
        } finally {
            ringBuffer.publish(sequence);
        }
    }
}

/**
 * 简单解析
 *
 * @author lixin
 */
class SimpleParserStage implements EventHandler<MessageEvent> {
    @Override
    public void onEvent(MessageEvent event, long sequence, boolean endOfBatch) {
        System.out.println("simple parse - [" + Thread.currentThread().getName() + "]" + event);
        event.setValue(event.getValue());
    }
}

/**
 * DML解析(WorkHandler:为多线程消费者需要定义的基类)
 *
 * @author lixin
 *
 */
class DmlParserStage implements WorkHandler<MessageEvent> {
    @Override
    public void onEvent(MessageEvent event) {
        System.out.println("dml parse - [" + Thread.currentThread().getName() + "]" + event);
        event.setValue(event.getValue());
    }
}

class SinkStoreStage implements EventHandler<MessageEvent> {

    @Override
    public void onEvent(MessageEvent event, long sequence, boolean endOfBatch) {
        System.out.println("sink - [" + Thread.currentThread().getName() + "]" + event);
    }
}

/**
 * 业务模型工厂.Disruptor在构建时,会创建一个环形队列,队列里的业务模型就是通过该工厂创建的
 *
 * @author lixin
 *
 */
class MessageEventFactory implements EventFactory<MessageEvent> {
    @Override
    public MessageEvent newInstance() {
        return new MessageEvent();
    }
}

/**
 * 业务模型
 *
 * @author lixin
 *
 */
class MessageEvent {
    private Long value;

    public Long getValue() {
        return value;
    }

    public void setValue(Long value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "MessageEvent [value=" + value + "]";
    }
}

/**
 * 异常处理
 *
 * @author lixin
 *
 */
class SimpleFatalExceptionHandler implements ExceptionHandler<MessageEvent> {
    @Override
    public void handleEventException(Throwable ex, long sequence, MessageEvent event) {
    }

    @Override
    public void handleOnStartException(Throwable ex) {
    }

    @Override
    public void handleOnShutdownException(Throwable ex) {

    }
}
