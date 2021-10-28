<?php

namespace ChrisHarvey\SimpleKafka;

use RuntimeException;
use RdKafka\Conf;
use RdKafka\Producer;
use RdKafka\KafkaConsumer;
use RecursiveArrayIterator;
use RecursiveIteratorIterator;

class Kafka
{
    public function __construct(
        protected array|string $brokers,
        protected array $config = []
    ) {
        if (is_string($brokers)) {
            $this->brokers = explode(',', $brokers);
        }
    }

    public function consume(string $groupId, string $topic, callable $callback, ?callable $timeoutCallback = null)
    {
        $conf = $this->getConf();

        $conf->set('group.id', $groupId);
        $conf->set('auto.offset.reset', 'earliest');

        $consumer = new KafkaConsumer($conf);

        $consumer->subscribe([$topic]);

        while (true) {
            $message = $consumer->consume(120*1000);
            switch ($message->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    $callback($message);
                break;

                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    $timeoutCallback && $timeoutCallback($message);
                break;

                default:
                    throw new \Exception($message->errstr(), $message->err);
                break;
            }
        }
    }

    public function produce(string $topicName, string $payload, int $partition = RD_KAFKA_PARTITION_UA, int $messageFlags = 0, int $maxFlushRetries = 10)
    {
        $producer = new Producer($this->getConf());

        $topic = $producer->newTopic($topicName);

        $topic->produce($partition, $messageFlags, $payload);
        $producer->poll(0);

        for ($flushRetries = 0; $flushRetries < $maxFlushRetries; $flushRetries++) {
            $result = $producer->flush(10000);

            if (RD_KAFKA_RESP_ERR_NO_ERROR === $result) {
                break;
            }
        }

        if (RD_KAFKA_RESP_ERR_NO_ERROR !== $result) {
            throw new RuntimeException('Was unable to flush, messages might be lost!');
        }
    }

    protected function getConf()
    {
        $conf = new Conf();

        $iterator = new RecursiveIteratorIterator(
            new RecursiveArrayIterator(
                $this->config
            )
        );
        
        foreach ($iterator as $value) {
            $keys = [];

            foreach (range(0, $iterator->getDepth()) as $depth) {
                $keys[] = $iterator->getSubIterator($depth)->key();
            }

            $conf->set(implode('.', $keys), $value);
        }

        $conf->set('metadata.broker.list', implode(',', $this->brokers));

        return $conf;
    }
}
