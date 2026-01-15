<?php

namespace AppKit\JobQueue;

use AppKit\StartStop\StartStopInterface;
use AppKit\Health\HealthIndicatorInterface;
use AppKit\Health\HealthCheckResult;
use AppKit\Json\Json;
use AppKit\Amqp\AmqpNackReject;
use AppKit\Amqp\AmqpNackRequeue;

use Throwable;

class JobQueue implements StartStopInterface, HealthIndicatorInterface {
    const AMQP_PREFIX = 'appkit_jobqueue';

    private $appId;
    private $amqp;

    private $log;
    private $isStarted = false;
    private $workerRestoreData = [];
    private $workerCtag = [];
    
    function __construct($appId, $log, $amqp) {
        $this -> appId = $appId;
        $this -> amqp = $amqp;

        $this -> log = $log -> withModule($this);
    }

    public function start() {
        try {
            $this -> amqp -> declareExchange(
                self::AMQP_PREFIX,
                'direct',
                false, // passive
                true, // durable
                false // autoDelete
            );
            $this -> log -> debug('Declared job queue exchange');
        } catch(Throwable $e) {
            $error = 'Failed to declare job queue exchange';
            $this -> log -> error($error, $e);
            throw new JobQueueException(
                $error,
                previous: $e
            );
        }

        $this -> amqp -> onConnect(function() {
            return $this -> onAmqpReconnect();
        });

        $this -> log -> info('Job queue is ready');

        $this -> isStarted = true;
    }

    public function stop() {
        $this -> isStarted = false;

        foreach($this -> workerRestoreData as $job => $_) {
            $this -> log -> warning("Worker $job is still active at shutdown");
            try {
                $this -> cancelWorker($job);
                $this -> log -> info("Canceled worker $job");
            } catch(Throwable $e) {
                $this -> log -> error("Failed to cancel worker $job", $e);
            }
        }
    }

    public function checkHealth() {
        return new HealthCheckResult([
            'AMQP client' => $this -> amqp,
            'Started' => $this -> isStarted
        ]);
    }

    public function enqueue($appId, $job, $body = [], $ttl = 0) {
        $headers = [
            'delivery_mode' => $ttl == 0 ? 2 : 1, // 1=transient, 2=persistent
            'expiration' => (string)($ttl * 1000)
        ];

        try {
            $bodyJson = Json::encode($body);
        } catch(Throwable $e) {
            throw new JobQueueException(
                'Failed to encode message: ' . $e -> getMessage(),
                previous: $e
            );
        }

        try {
            return $this -> amqp -> publish(
                $bodyJson,
                $headers,
                self::AMQP_PREFIX,
                "${appId}_$job",
                true, // mandatory
                confirm: true
            );
        } catch(Throwable $e) {
            $error = 'Failed to publish AMQP message';
            $this -> log -> error($error, $e);
            throw new JobQueueException(
                $error,
                previous: $e
            );
        }
    }

    public function work(
        $job,
        $handler,
        $concurrency = 1,
        $prefetchCount = null
    ) {
        if(isset($this -> workerRestoreData[$job]))
            throw new JobQueueException("Worker $job already exists");

        $workerRestoreData = [
            $job,
            $handler,
            $concurrency,
            $prefetchCount
        ];

        $this -> workInternal(...$workerRestoreData);
        $this -> workerRestoreData[$job] = $workerRestoreData;

        $this -> log -> debug("New worker: $job");

        return $this;
    }

    public function cancelWorker($job) {
        if(!isset($this -> workerRestoreData[$job]))
            throw new JobQueueException("Worker $job does not exist");

        if(isset($this -> workerCtag[$job])) {
            $this -> cancelWorkerInternal($this -> workerCtag[$job]);
            unset($this -> workerCtag[$job]);
        }

        unset($this -> workerRestoreData[$job]);
        $this -> log -> debug("Cleaned up worker $job");

        return $this;
    }

    private function workInternal(
        $job,
        $handler,
        $concurrency,
        $prefetchCount
    ) {
        $routingKey = $this -> appId . "_$job";
        $queue = self::AMQP_PREFIX . "_$routingKey";
        if(strlen($queue) > 255)
            $queue = self::AMQP_PREFIX . '_' . hash('sha256', $routingKey);

        try {
            $this -> amqp -> declareQueue(
                $queue,
                false, // passive
                true, // durable
                false, // exclusive
                false // autoDelete
            );
            $this -> log -> debug("Declared queue $queue");
        } catch(Throwable $e) {
            $error = 'Failed to declare queue';
            $this -> log -> error("$error $queue", $e);
            throw new JobQueueException($error, previous: $e);
        }

        try {
            $logMsg = "$queue to exchange " . self::AMQP_PREFIX . " by routing key $routingKey";
            $this -> amqp -> bindQueue(
                $queue,
                self::AMQP_PREFIX,
                $routingKey
            );
            $this -> log -> debug("Bound queue $logMsg");
        } catch(Throwable $e) {
            $error = 'Failed to bind queue';
            $this -> log -> error("$error $logMsg", $e);
            throw new JobQueueException($error, previous: $e);
        }

        try {
            $ctag = $this -> amqp -> consume(
                $queue,
                function($body, $headers) use($handler, $job) {
                    return $this -> handleMessage($body, $headers, $handler, $job);
                },
                concurrency: $concurrency,
                prefetchCount: $prefetchCount
            );
            $this -> log -> debug("Consumed queue $queue, consumer tag: $ctag");
        } catch(Throwable $e) {
            $error = 'Failed to consume queue';
            $this -> log -> error("$error $queue");
            throw new JobQueueException($error, previous: $e);
        }

        $this -> workerCtag[$job] = $ctag;
    }

    private function cancelWorkerInternal($ctag) {
        try {
            $this -> amqp -> cancelConsumer($ctag);
            $this -> log -> debug("Canceled consumer $ctag");
        } catch(Throwable $e) {
            $error = "Failed to cancel consumer";
            $this -> log -> error("$error $ctag", $e);
            throw new JobQueueException($error, previous: $e);
        }
    }

    private function onAmqpReconnect() {
        $this -> log -> warning("Detected AMQP client reconnect, restoring all workers...");

        foreach($this -> workerRestoreData as $job => $restoreData) {
            try {
                $this -> workInternal(...$restoreData);
                $this -> log -> info("Restored worker $job");
            } catch(Throwable $e) {
                $this -> log -> error("Failed to restore worker $job", $e);
            }
        }
    }

    private function handleMessage($bodyJson, $headers, $handler, $job) {
        $messageIdStr = $headers['message-id'] ?? 'missing message-id';

        try {
            $body = Json::decode($bodyJson);
        } catch(Throwable $e) {
            $this -> log -> warning(
                "Rejecting corrupted job ${job}[$messageIdStr]",
                $e
            );
            throw new AmqpNackReject(
                "Failed to decode message",
                previous: $e
            );
        }

        try {
            $handler($body);
        } catch(Throwable $e) {
            $this -> log -> error(
                "Requeuing failed job ${job}[$messageIdStr]",
                $e
            );
            throw new AmqpNackRequeue(
                "Job failed",
                previous: $e
            );
        }
    }
}
