<?php

namespace AppKit\JobQueue;

use AppKit\StartStop\StartStopInterface;
use AppKit\Health\HealthIndicatorInterface;
use AppKit\Health\HealthCheckResult;
use AppKit\Json\Json;
use AppKit\Amqp\AmqpNackReject;
use AppKit\Amqp\AmqpNackRequeue;
use function AppKit\Async\async;

use Throwable;

class JobQueue implements StartStopInterface, HealthIndicatorInterface {
    const AMQP_PREFIX = 'ak_jq';

    private $appId;
    private $amqp;

    private $log;
    private $isStarted = false;
    private $workerRestoreData = [];
    private $workerCtag = [];
    
    function __construct($appId, $log, $amqp) {
        $this -> appId = $appId;
        $this -> amqp = $amqp;

        $this -> log = $log -> withModule(static::class);
    }

    public function start() {
        $exchange = self::AMQP_PREFIX;

        try {
            $this -> amqp -> declareExchange(
                $exchange,
                'direct',
                false, // passive
                true, // durable
                false // autoDelete
            );
            $this -> log -> debug(
                'Declared exchange',
                [ 'exchange' => $exchange ]
            );
        } catch(Throwable $e) {
            $error = 'Failed to declare exchange';
            $this -> log -> error(
                $error,
                [ 'exchange' => $exchange ],
                $e
            );
            throw new JobQueueException(
                $error,
                previous: $e
            );
        }

        $this -> amqp -> on('connect', async(function() {
            return $this -> onAmqpReconnect();
        }));

        $this -> log -> info('Job queue is ready');

        $this -> isStarted = true;
    }

    public function stop() {
        $this -> isStarted = false;

        foreach($this -> workerRestoreData as $job => $_) {
            $this -> log -> warning(
                'Worker is still active at shutdown',
                [ 'job' => $job ]
            );
            try {
                $this -> cancelWorker($job);
                $this -> log -> info(
                    'Canceled worker',
                    [ 'job' => $job ]
                );
            } catch(Throwable $e) {
                $this -> log -> error(
                    'Failed to cancel worker',
                    [ 'job' => $job ],
                    $e
                );
            }
        }
    }

    public function checkHealth() {
        return new HealthCheckResult([
            'AMQP client' => $this -> amqp,
            'Started' => $this -> isStarted
        ]);
    }

    public function dispatch($appId, $job, $body = [], $ttl = 0) {
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

        $exchange = self::AMQP_PREFIX;
        $routingKey = $appId . '_' . $job;
        try {
            return $this -> amqp -> publish(
                $bodyJson,
                $headers,
                $exchange,
                $routingKey,
                true, // mandatory
                confirm: true
            );
        } catch(Throwable $e) {
            $error = 'Failed to publish AMQP message';
            $this -> log -> error(
                $error,
                [
                    'appId' => $appId,
                    'job' => $job,
                    'headers' => $headers,
                    'exchange' => $exchange,
                    'routingKey' => $routingKey
                ],
                $e
            );
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
            throw new JobQueueException("Already working on job $job");

        $workerRestoreData = [
            $handler,
            $concurrency,
            $prefetchCount
        ];

        $this -> workInternal($job, ...$workerRestoreData);
        $this -> workerRestoreData[$job] = $workerRestoreData;

        $this -> log -> debug(
            'Created worker',
            [
                'job' => $job,
                'concurrency' => $concurrency,
                'prefetchCount' => $prefetchCount
            ]
        );

        return $this;
    }

    public function cancelWorker($job) {
        if(!isset($this -> workerRestoreData[$job]))
            throw new JobQueueException("No active worker for job $job");

        if(isset($this -> workerCtag[$job])) {
            $this -> cancelWorkerInternal($this -> workerCtag[$job], $job);
            unset($this -> workerCtag[$job]);
        }

        unset($this -> workerRestoreData[$job]);
        $this -> log -> debug(
            'Cleaned up worker',
            [ 'job' => $job ]
        );

        return $this;
    }

    private function workInternal(
        $job,
        $handler,
        $concurrency,
        $prefetchCount
    ) {
        $routingKey = $this -> appId . '_' . $job;
        $queue = self::AMQP_PREFIX . '_' . $routingKey;
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
            $this -> log -> debug(
                'Declared worker queue',
                [ 'queue' => $queue, 'job' => $job ]
            );
        } catch(Throwable $e) {
            $error = 'Failed to declare worker queue';
            $this -> log -> error(
                $error,
                [ 'queue' => $queue, 'job' => $job ],
                $e
            );
            throw new JobQueueException($error, previous: $e);
        }

        $exchange = self::AMQP_PREFIX;

        try {
            $this -> amqp -> bindQueue(
                $queue,
                $exchange,
                $routingKey
            );
            $this -> log -> debug(
                'Bound worker queue',
                [
                    'queue' => $queue,
                    'exchange' => $exchange,
                    'routingKey' => $routingKey,
                    'job' => $job
                ]
            );
        } catch(Throwable $e) {
            $error = 'Failed to bind worker queue';
            $this -> log -> error(
                $error,
                [
                    'queue' => $queue,
                    'exchange' => $exchange,
                    'routingKey' => $routingKey,
                    'job' => $job
                ],
                $e
            );
            throw new JobQueueException($error, previous: $e);
        }

        try {
            $consumerTag = $this -> amqp -> consume(
                $queue,
                function($body, $headers) use($handler, $job) {
                    return $this -> handleMessage($body, $headers, $handler, $job);
                },
                concurrency: $concurrency,
                prefetchCount: $prefetchCount
            );
            $this -> log -> debug(
                'Started consumer',
                [
                    'consumerTag' => $consumerTag,
                    'queue' => $queue,
                    'concurrency' => $concurrency,
                    'prefetchCount' => $prefetchCount,
                    'job' => $job
                ]
            );
        } catch(Throwable $e) {
            $error = 'Failed to start consumer';
            $this -> log -> error(
                $error,
                [
                    'queue' => $queue,
                    'concurrency' => $concurrency,
                    'prefetchCount' => $prefetchCount,
                    'job' => $job
                ],
                $e
            );
            throw new JobQueueException($error, previous: $e);
        }

        $this -> workerCtag[$job] = $consumerTag;
    }

    private function cancelWorkerInternal($consumerTag, $job) {
        try {
            $this -> amqp -> cancelConsumer($consumerTag);
            $this -> log -> debug(
                'Canceled consumer',
                [
                    'consumerTag' => $consumerTag,
                    'job' => $job
                ]
            );
        } catch(Throwable $e) {
            $error = 'Failed to cancel consumer';
            $this -> log -> error(
                $error,
                [
                    'consumerTag' => $consumerTag,
                    'job' => $job
                ],
                $e
            );
            throw new JobQueueException($error, previous: $e);
        }
    }

    private function onAmqpReconnect() {
        $this -> log -> warning("Detected AMQP client reconnect, restoring all workers");

        foreach($this -> workerRestoreData as $job => $restoreData) {
            try {
                $this -> workInternal($job, ...$restoreData);
                $this -> log -> info(
                    'Restored worker',
                    [ 'job' => $job ]
                );
            } catch(Throwable $e) {
                $this -> log -> error(
                    'Failed to restore worker',
                    [ 'job' => $job ],
                    $e
                );
            }
        }
    }

    private function handleMessage($bodyJson, $headers, $handler, $job) {
        $this -> log -> setContext('jqJob', $job);

        try {
            $body = Json::decode($bodyJson);
        } catch(Throwable $e) {
            $error = 'Failed to decode message';
            $this -> log -> warning($error, $e);
            throw new AmqpNackReject($error, previous: $e);
        }

        try {
            $handler($body);
        } catch(Throwable $e) {
            $this -> log -> error(
                'Uncaught worker exception, requeuing task',
                $e
            );
            throw new AmqpNackRequeue(
                'Worker exception',
                previous: $e
            );
        }
    }
}
